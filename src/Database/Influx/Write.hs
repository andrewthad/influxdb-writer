{-# language BangPatterns #-}
{-# language DataKinds #-}
{-# language NamedFieldPuns #-}
{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language ScopedTypeVariables #-}
{-# language TypeApplications #-}

{-| This module provides an API for writing InfluxDB line protocol
    'Point's to InfluxDB. To create 'Point's, see
    "Database.Influx.LineProtocol".

    There are two ways to use this API: bracketed (using 'with') and
    unbracketed (using 'open' and 'close'). It is strongly recommended
    to use the bracketed 'with' unless you are writing some API on
    top of these functions.
 -}

module Database.Influx.Write
  ( -- * Channel
    Influx
  , with
  , withBasicAuth
    -- * Time-Series Metrics
  , Database(..)
  , write
    -- * Unbracketed Channel
  , open
  , openBasicAuth
  , close
    -- * Explicit Connection
  , connected
  , disconnected
    -- * Exceptions
  , InfluxException(..)
  ) where

import Control.Exception (mask,onException)
import Control.Monad.ST (ST,runST)
import Control.Monad.Trans.Except (ExceptT(ExceptT),runExceptT)
import Data.Bytes.Types (MutableBytes(..),Bytes)
import Data.ByteString (ByteString)
import Data.Char (chr,ord)
import Data.Primitive (ByteArray,MutableByteArray)
import Data.Primitive.ByteArray.Offset (MutableByteArrayOffset(..))
import Data.Primitive.Unlifted.Array (MutableUnliftedArray(..))
import Data.Primitive.Unlifted.Array (UnliftedArray)
import Data.Vector (Vector)
import Data.Word (Word8,Word16,Word64)
import Database.Influx.LineProtocol(Point,encodePoint)
import Foreign.C.Types (CInt)
import Foreign.Ptr (Ptr)
import GHC.Exts (Ptr(..))
import GHC.Exts (RealWorld,Addr#)
import Net.Types (IPv4(..))
import Socket.Stream.IPv4 (Connection,Peer(..))
import Socket.Stream.IPv4 (Family(Internet),Version(V4))
import Socket.Stream.IPv4 (Interruptibility(Uninterruptible),CloseException)
import qualified Arithmetic.Nat as Nat
import qualified Data.ByteString.Unsafe as BU
import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Base64 as Base64
import qualified Data.Bytes.Builder as BB
import qualified Data.Bytes.Builder as Builder
import qualified Data.Bytes.Builder.Bounded as Bounded
import qualified Data.Bytes.Builder.Unsafe as BBU
import qualified Data.Bytes.Chunks as Chunks
import qualified Data.Primitive as PM
import qualified Data.Primitive.Addr as PM
import qualified Data.Primitive.ByteArray.Unaligned as PM
import qualified Data.Primitive.Ptr as PM
import qualified Data.Primitive.Unlifted.Array as PM
import qualified Data.Vector as V
import qualified GHC.Exts as Exts
import qualified Net.IPv4 as IPv4
import qualified Socket.Stream.IPv4 as SCK
import qualified Socket.Stream.Uninterruptible.Bytes as SB
import qualified Socket.Stream.Uninterruptible.MutableBytes as SMB
import qualified System.IO as IO

fromByteString :: ByteString -> IO ByteArray
fromByteString b = BU.unsafeUseAsCStringLen b $ \((Ptr addr#),sz) -> do
  m <- PM.newByteArray sz
  PM.copyAddrToByteArray m 0 (PM.Addr addr#) sz
  PM.unsafeFreezeByteArray m

-- Implementation Notes:
-- InfluxDB does not support http request pipelining. Consequently,
-- we always wait for a response before sending the next request.
-- We use chunked encoding rather aggressively to try to improve
-- locality. We continually render the data points to the same
-- mutable byte array before sending them out.

-- | An exception while writing to InfluxDB. This includes
-- several stream socket exceptions (connecting or sending
-- could fail). It also includes the possibility that the
-- HTTP response code is something other than 204.
data InfluxException
  = ConnectException (SCK.ConnectException ('Internet 'V4) 'Uninterruptible) -- ^ There was an exception encountered when trying to connection to an 'Influx' 'Database'.
  | SendException (SCK.SendException 'Uninterruptible) -- ^ There was an exception when trying to send something to an 'Influx' 'Database'.
  | ReceiveException (SCK.ReceiveException 'Uninterruptible) -- ^ There was an exception encountered when trying to receive a response from an 'Influx' 'Database'.
  | ResponseException -- ^ We received a non-204 HTTP response code.
  deriving (Eq, Show)

-- | A connection to our 'Influx' instance.
data Influx = Influx
  !(MutableByteArray RealWorld)
  -- Inside this mutable byte array, we have:
  -- 1. Reconnection count: Word64
  -- 2. Active connection: SCK.Connection (CInt), given 64 bits of space
  -- 3. Peer IPv4 (static): Word32
  -- 4. Peer Port (static): Word16
  -- 5. Local Port: Word16
  !ByteArray
  -- Hostname of the peer
  !ByteArray
  -- A header with the authentication credentials. This is either empty
  -- (no authentication) or trailed by CR-LF.

-- | An InfluxDB database name represented as UTF-8 encoded text.
-- InfluxDB does not document any restrictions on the name of a database.
-- However, InfluxDB has a history of issues when using symbols
-- like slash or underscore in database names. Try to avoid these
-- characters.
newtype Database = Database ByteArray

c2w :: Char -> Word8
{-# inline c2w #-}
c2w = fromIntegral . ord

w2c :: Word8 -> Char
{-# inline w2c #-}
w2c = chr . fromIntegral

-- This always starts writing at postion 0 in the destination
-- buffer. This includes both the request line and the headers.
encodeHttpHeaders ::
     ByteArray -- Encoded host
  -> ByteArray -- Database
  -> ByteArray -- Auth header
  -> ByteArray
encodeHttpHeaders !host !db !authHdr = runST $ do
  let requiredBytes = 0
        + PM.sizeofByteArray host
        + PM.sizeofByteArray db
        + PM.sizeofByteArray authHdr
        + ( staticRequestHeadersLen
          + ( 28 -- http method and most of url
            + 2 -- trailing CRLFx1
            )
          )
  arr <- PM.newByteArray requiredBytes
  PM.writeByteArray arr 0 (c2w 'P')
  PM.writeByteArray arr 1 (c2w 'O')
  PM.writeByteArray arr 2 (c2w 'S')
  PM.writeByteArray arr 3 (c2w 'T')
  PM.writeByteArray arr 4 (c2w ' ')
  PM.writeByteArray arr 5 (c2w '/')
  PM.writeByteArray arr 6 (c2w 'w')
  PM.writeByteArray arr 7 (c2w 'r')
  PM.writeByteArray arr 8 (c2w 'i')
  PM.writeByteArray arr 9 (c2w 't')
  PM.writeByteArray arr 10 (c2w 'e')
  PM.writeByteArray arr 11 (c2w '?')
  PM.writeByteArray arr 12 (c2w 'p')
  PM.writeByteArray arr 13 (c2w 'r')
  PM.writeByteArray arr 14 (c2w 'e')
  PM.writeByteArray arr 15 (c2w 'c')
  PM.writeByteArray arr 16 (c2w 'i')
  PM.writeByteArray arr 17 (c2w 's')
  PM.writeByteArray arr 18 (c2w 'i')
  PM.writeByteArray arr 19 (c2w 'o')
  PM.writeByteArray arr 20 (c2w 'n')
  PM.writeByteArray arr 21 (c2w '=')
  PM.writeByteArray arr 22 (c2w 'n')
  PM.writeByteArray arr 23 (c2w 's')
  PM.writeByteArray arr 24 (c2w '&')
  PM.writeByteArray arr 25 (c2w 'd')
  PM.writeByteArray arr 26 (c2w 'b')
  PM.writeByteArray arr 27 (c2w '=')
  i0 <- copySmall arr 28 db
  PM.copyAddrToByteArray arr i0 staticRequestHeaders staticRequestHeadersLen
  let i1 = i0 + staticRequestHeadersLen
  i2 <- copySmall arr i1 host
  PM.writeByteArray arr i2 (c2w '\r')
  PM.writeByteArray arr (i2 + 1) (c2w '\n')
  PM.copyByteArray arr (i2 + 2) authHdr 0 (PM.sizeofByteArray authHdr)
  PM.unsafeFreezeByteArray arr

-- Used internally. When debugging problems, it can be helpful
-- to set this to a lower number.
minimumBufferSize :: Int
minimumBufferSize = 8192 - (2 * PM.sizeOf (undefined :: Int))

staticRequestHeaders :: PM.Addr
staticRequestHeaders = PM.Addr
  " HTTP/1.1\r\n\
  \Content-Type:text/plain; charset=utf-8\r\n\
  \Transfer-Encoding:chunked\r\n\
  \Host:"#

-- It is silly that we have to do this. See
-- https://gitlab.haskell.org/ghc/ghc/issues/5218
staticRequestHeadersLen :: Int
{-# NOINLINE staticRequestHeadersLen #-}
staticRequestHeadersLen = case staticRequestHeaders of
  PM.Addr x -> cstringLen (Ptr x)

cstringLen :: Ptr Word8 -> Int
cstringLen ptr = go 0 where
  go !ix = if PM.indexOffPtr ptr ix == 0
    then ix
    else go (ix + 1)

copySmall ::
     MutableByteArray s
  -> Int
  -> ByteArray
  -> ST s Int
{-# inline copySmall #-}
copySmall dst doff0 src = do
  let sz = PM.sizeofByteArray src
  PM.copyByteArray dst doff0 src 0 sz
  pure (doff0 + sz)

metadataSize :: Int
metadataSize = 24

incrementConnectionCount :: Influx -> IO ()
incrementConnectionCount (Influx arr _ _) = do
  n :: Word64 <- PM.readUnalignedByteArray arr 0
  PM.writeUnalignedByteArray arr 0 (n + 1)

writeActiveConnection :: Influx -> Connection -> IO ()
writeActiveConnection (Influx arr _ _) (SCK.Connection c) =
  PM.writeUnalignedByteArray arr 8 c

writeLocalPort :: Influx -> Word16 -> IO ()
writeLocalPort (Influx arr _ _) =
  PM.writeUnalignedByteArray arr 22

readPeerIPv4 :: Influx -> IO IPv4
readPeerIPv4 (Influx arr _ _) = do
  w <- PM.readUnalignedByteArray arr 16
  pure (IPv4 w)

readPeerPort :: Influx -> IO Word16
readPeerPort (Influx arr _ _) = PM.readUnalignedByteArray arr 20

readActiveConnection :: Influx -> IO (Maybe Connection)
readActiveConnection (Influx arr _ _) = do
  w :: Word16 <- PM.readUnalignedByteArray arr 22
  case w of
    0 -> pure Nothing
    _ -> do
      c <- PM.readUnalignedByteArray arr 8
      pure (Just (SCK.Connection c))

-- | Open a connection to an 'Influx' instance.
open :: Peer -> IO Influx
open Peer{address,port} = do
  arr <- PM.newByteArray metadataSize
  PM.writeUnalignedByteArray arr 0 (0 :: Word64)
  PM.writeUnalignedByteArray arr 8 ((-1) :: CInt)
  PM.writeUnalignedByteArray arr 16 (getIPv4 address)
  PM.writeUnalignedByteArray arr 20 port
  -- The port needs to be zero. Other functions interpret the
  -- zero port to mean that there is no active connection.
  PM.writeUnalignedByteArray arr 22 (0 :: Word16)
  peerDescr <- fromByteString (IPv4.encodeUtf8 address)
  pure (Influx arr peerDescr mempty)

-- | Open a connection to an 'Influx' instance using BasicAuth.
openBasicAuth ::
     Peer
  -> Bytes -- ^ Username
  -> Bytes -- ^ Password
  -> IO Influx
openBasicAuth Peer{address,port} !user !pass = do
  arr <- PM.newByteArray metadataSize
  PM.writeUnalignedByteArray arr 0 (0 :: Word64)
  PM.writeUnalignedByteArray arr 8 ((-1) :: CInt)
  PM.writeUnalignedByteArray arr 16 (getIPv4 address)
  PM.writeUnalignedByteArray arr 20 port
  -- The port needs to be zero. Other functions interpret the
  -- zero port to mean that there is no active connection.
  PM.writeUnalignedByteArray arr 22 (0 :: Word16)
  peerDescr <- fromByteString (IPv4.encodeUtf8 address)
  pure (Influx arr peerDescr (buildBasicAuthHeader user pass))

buildBasicAuthHeader :: Bytes -> Bytes -> ByteArray
buildBasicAuthHeader !user !pass = Chunks.concatU $ Builder.run 256 $
  Builder.cstring (Ptr "Authorization: Basic "# ) <>
  Base64.builder (Bytes.intercalateByte2 0x2E user pass) <>
  Builder.fromBounded Nat.two (Bounded.append (Bounded.ascii '\r') (Bounded.ascii '\n'))

-- | Close a connection to an 'Influx' instance.
close :: Influx -> IO (Either SCK.CloseException ())
close = disconnected

-- | Write a vector of data points to InfluxDB. Returns an exception
-- if an underlying stream-socket operation fails or if InfluxDB
-- returns a non-204 response code.
write ::
     Influx -- ^ Connection to InfluxDB
  -> Database -- ^ Database name
  -> Vector Point -- ^ Data points
  -> IO (Either InfluxException ())
write !inf@(Influx _ peerDescr authHdr) (Database db) !points0 = case V.length points0 of
  0 -> pure (Right ())
  _ -> getConnection inf >>= \case
    Left err -> pure $! Left $! ConnectException err
    Right conn -> do
      -- TODO: escape the database name correctly
      let host = peerDescr
          bufSz0 = minimumBufferSize
          headers = encodeHttpHeaders host db authHdr
      r <- runExceptT $ do
        ExceptT (SB.send conn (Bytes.fromByteArray headers))
        -- The headers have been sent. We must now send all the points.
        BB.putManyConsLength
          (Nat.constant @20)
          (\sz -> Bounded.ascii '\r'
            `Bounded.append` Bounded.ascii '\n'
            `Bounded.append` Bounded.word64PaddedUpperHex (fromIntegral sz)
            `Bounded.append` Bounded.ascii '\r'
            `Bounded.append` Bounded.ascii '\n'
          )
          8176
          encodePoint
          points0
          (ExceptT . SMB.send conn)
        ExceptT (SB.send conn (Bytes.fromByteArray terminator))
      case r of
        Left err -> do
          writeLocalPort inf 0
          SCK.disconnect_ conn
          pure (Left (SendException err))
        Right (_ :: ()) -> receiveResponseStage1 conn

terminator :: ByteArray
terminator = runST $ do
  arr <- PM.newByteArray 7
  PM.writeByteArray arr 0 (c2w '\r')
  PM.writeByteArray arr 1 (c2w '\n')
  PM.writeByteArray arr 2 (c2w '0')
  PM.writeByteArray arr 3 (c2w '\r')
  PM.writeByteArray arr 4 (c2w '\n')
  PM.writeByteArray arr 5 (c2w '\r')
  PM.writeByteArray arr 6 (c2w '\n')
  PM.unsafeFreezeByteArray arr

-- | Ensure that a connection to InfluxDB is open. If a connection
-- is already active, this has no effect. Otherwise, it attempts to
-- open a stream socket, reporting any errors to the user. Note that
-- it is not necessary to call this function in order to use this
-- library. All of the functions that communicate with InfluxDB will
-- open a connection if one is not already active. This function is
-- intended to be used to check at load time that the InfluxDB host
-- is accepting connections. It is often useful to terminate the
-- application immidiately in this situation.
connected ::
     Influx
  -> IO (Either (SCK.ConnectException ('Internet 'V4) 'Uninterruptible) ())
connected inf = readActiveConnection inf >>= \case
  Nothing -> connect inf >>= \case
    Left err -> pure (Left err)
    Right _ -> pure (Right ())
  Just _ -> pure (Right ())

getConnection ::
     Influx
  -> IO (Either (SCK.ConnectException ('Internet 'V4) 'Uninterruptible) Connection)
getConnection inf = readActiveConnection inf >>= \case
  Nothing -> connect inf >>= \case
    Left err -> pure (Left err)
    Right conn -> pure (Right conn)
  Just conn -> pure (Right conn)

-- Precondition: there is not already an established connection.
connect ::
     Influx
  -> IO (Either (SCK.ConnectException ('Internet 'V4) 'Uninterruptible) Connection)
connect !inf = do
  address <- readPeerIPv4 inf
  port <- readPeerPort inf
  -- TODO: mask exceptions
  SCK.connect (Peer{address,port}) >>= \case
    Left err -> pure (Left err)
    Right conn -> do
      writeActiveConnection inf conn
      -- TODO: Write the real local port, possibly. With sockets,
      -- we do not actually have access to it.
      writeLocalPort inf (1 :: Word16)
      incrementConnectionCount inf
      pure (Right conn)

-- | Ensure that there is not an open connection to InfluxDB.
-- If there is a connection, this closes it. If not, this
-- function has no effect. Note that it is not necessary to
-- call this function in order to use this library. It useful
-- as an optimization in either of these scenarios:
--
-- * The user knows that another request will not be made for a
--   long time.
-- * There is a load balancer in front of an InfluxDB cluster.
disconnected :: Influx -> IO (Either SCK.CloseException ())
disconnected inf = readActiveConnection inf >>= \case
  Nothing -> pure (Right ())
  Just conn -> do
    writeLocalPort inf 0
    SCK.disconnect conn

-- | Perform some 'IO' operation by connecting to an 'Influx' instance.
--   This will catch any exceptions and ensure that the connection to
--   'Influx' is closed. Returns both the value returned by your action
--   and an 'SCK.CloseException', if one occured.
--
--   /Note/: It is strongly preferred that you use 'with' over calling
--   'open' or 'close' directly, unless you are writing some API over those
--   two functions.
with ::
     Peer -- ^ The Peer where the 'Influx' instance is located.
  -> (Influx -> IO a) -- ^ Apply this function to the connection to
                      --   our 'Influx' instance
  -> IO (Either SCK.CloseException (), a)
with p f = do
  inf <- open p
  mask $ \restore -> do
    a <- onException (restore (f inf)) (disconnected inf)
    e <- disconnected inf
    pure (e,a)

-- | Variant of 'with' that uses BasicAuth to authenticate.
withBasicAuth ::
     Peer -- ^ The Peer where the 'Influx' instance is located.
  -> Bytes -- ^ User
  -> Bytes -- ^ Password
  -> (Influx -> IO a) -- ^ Apply this function to the connection to
                      --   our 'Influx' instance
  -> IO (Either SCK.CloseException (), a)
withBasicAuth !p !user !pass f = do
  inf <- openBasicAuth p user pass
  mask $ \restore -> do
    a <- onException (restore (f inf)) (disconnected inf)
    e <- disconnected inf
    pure (e,a)

receiveResponseStage1 :: Connection -> IO (Either InfluxException ())
receiveResponseStage1 conn = do
  let sz = 1008
  arr <- PM.newByteArray sz
  SMB.receiveBetween conn (MutableBytes arr 0 sz) 12 >>= \case
    Left err -> pure $! Left $! ReceiveException $! err
    Right n -> do
      c0 <- PM.readByteArray arr 0
      c1 <- PM.readByteArray arr 1
      c2 <- PM.readByteArray arr 2
      c3 <- PM.readByteArray arr 3
      c4 <- PM.readByteArray arr 4
      c5 <- PM.readByteArray arr 5
      c6 <- PM.readByteArray arr 6
      c7 <- PM.readByteArray arr 7
      c8 <- PM.readByteArray arr 8
      c9 <- PM.readByteArray arr 9
      c10 <- PM.readByteArray arr 10
      c11 <- PM.readByteArray arr 11
      let success =
               c2w 'H' == c0
            && c2w 'T' == c1
            && c2w 'T' == c2
            && c2w 'P' == c3
            && c2w '/' == c4
            && c2w '1' == c5
            && c2w '.' == c6
            && c2w '1' == c7
            && c2w ' ' == c8
            && c2w '2' == c9
            && c2w '0' == c10
            && c2w '4' == c11
      if success
        then receiveResponseStage2 conn arr n sz
        else do
          putStrLn $ map w2c [c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11]
          pure $! Left ResponseException

-- Precondition: ix >= 4
receiveResponseStage2 ::
     Connection
  -> MutableByteArray RealWorld
  -> Int -- Index
  -> Int -- Total size
  -> IO (Either InfluxException ())
receiveResponseStage2 !sock !buffer !ix !sz = if ix < sz
  then do
    (w4 :: Word8) <- PM.readByteArray buffer (ix - 1)
    (w3 :: Word8) <- PM.readByteArray buffer (ix - 2)
    (w2 :: Word8) <- PM.readByteArray buffer (ix - 3)
    (w1 :: Word8) <- PM.readByteArray buffer (ix - 4)
    if w1 == 13 && w2 == 10 && w3 == 13 && w4 == 10
      then pure $! Right ()
      else do
        let remaining = sz - ix
        SMB.receiveOnce sock (MutableBytes buffer 0 remaining) >>= \case
          Left err -> pure $! Left $! ReceiveException $! err
          Right n -> receiveResponseStage2 sock buffer (ix + n) sz
  else do
    buffer' <- PM.resizeMutableByteArray buffer (sz * 2)
    receiveResponseStage2 sock buffer' ix (sz * 2)
