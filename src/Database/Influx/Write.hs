{-# language BangPatterns #-}
{-# language DataKinds #-}
{-# language NamedFieldPuns #-}
{-# language DuplicateRecordFields #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language ScopedTypeVariables #-}

module Database.Influx.Write
  ( -- * Channel
    Influx
  , with
    -- * Time-Series Metrics
  , Database(..)
  , write
    -- * Unbracketed Channel
  , open
  , close
    -- * Explicit Connection
  , connected
  , disconnected
    -- * Exceptions
  , InfluxException(..)
  ) where

import Control.Exception (mask,onException)
import Control.Monad.ST (ST)
import Data.Bytes.Types (MutableBytes(..))
import Data.ByteString (ByteString)
import Data.Char (ord)
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
import qualified Data.ByteArray.Builder.Small as BB
import qualified Data.ByteArray.Builder.Small.Unsafe as BBU
import qualified Data.ByteString.Unsafe as BU
import qualified Data.Primitive as PM
import qualified Data.Primitive.Addr as PM
import qualified Data.Primitive.ByteArray.Unaligned as PM
import qualified Data.Primitive.Ptr as PM
import qualified Data.Primitive.Unlifted.Array as PM
import qualified Data.Vector as V
import qualified GHC.Exts as Exts
import qualified Net.IPv4 as IPv4
import qualified Socket.Stream.IPv4 as SCK
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
  = ConnectException (SCK.ConnectException ('Internet 'V4) 'Uninterruptible)
  | SendException (SCK.SendException 'Uninterruptible)
  | ReceiveException (SCK.ReceiveException 'Uninterruptible)
  | ResponseException

data Influx = Influx
  !(MutableByteArray RealWorld)
  -- Inside this mutable byte array, we have:
  -- * Reconnection count: Word64
  -- * Active connection: SCK.Connection (CInt), given 64 bits of space
  -- * Peer IPv4 (static): Word32
  -- * Peer Port (static): Word16
  -- * Local Port: Word16
  !ByteArray -- Hostname of the peer
  !(MutableUnliftedArray RealWorld (MutableByteArray RealWorld))

-- | An InfluxDB database name represented as UTF-8 encoded text.
-- InfluxDB does not document any restrictions on the name of a database.
-- However, InfluxDB has a history of issues when using symbols
-- like slash or underscore in database names. Try to avoid these
-- characters.
newtype Database = Database ByteArray

c2w :: Char -> Word8
{-# inline c2w #-}
c2w = fromIntegral . ord

-- This always starts writing at postion 0 in the destination
-- buffer. This includes both the request line and the headers.
encodeHttpHeaders ::
     ByteArray -- Encoded host
  -> ByteArray -- Database
  -> BB.Builder
encodeHttpHeaders host db = BB.construct $ \(MutableBytes arr off len) -> do
  let requiredBytes = 0
        + PM.sizeofByteArray host
        + PM.sizeofByteArray db
        + ( staticRequestHeadersLen
          + ( 28 -- http method and most of url
            + 4 -- trailing CRLFx2
            )
          )
  if requiredBytes <= len
    then do
      PM.writeByteArray arr (off + 0) (c2w 'P')
      PM.writeByteArray arr (off + 1) (c2w 'O')
      PM.writeByteArray arr (off + 2) (c2w 'S')
      PM.writeByteArray arr (off + 3) (c2w 'T')
      PM.writeByteArray arr (off + 4) (c2w ' ')
      PM.writeByteArray arr (off + 5) (c2w '/')
      PM.writeByteArray arr (off + 6) (c2w 'w')
      PM.writeByteArray arr (off + 7) (c2w 'r')
      PM.writeByteArray arr (off + 8) (c2w 'i')
      PM.writeByteArray arr (off + 9) (c2w 't')
      PM.writeByteArray arr (off + 10) (c2w 'e')
      PM.writeByteArray arr (off + 11) (c2w '?')
      PM.writeByteArray arr (off + 12) (c2w 'p')
      PM.writeByteArray arr (off + 13) (c2w 'r')
      PM.writeByteArray arr (off + 14) (c2w 'e')
      PM.writeByteArray arr (off + 15) (c2w 'c')
      PM.writeByteArray arr (off + 16) (c2w 'i')
      PM.writeByteArray arr (off + 17) (c2w 's')
      PM.writeByteArray arr (off + 18) (c2w 'i')
      PM.writeByteArray arr (off + 19) (c2w 'o')
      PM.writeByteArray arr (off + 20) (c2w 'n')
      PM.writeByteArray arr (off + 21) (c2w '=')
      PM.writeByteArray arr (off + 22) (c2w 'n')
      PM.writeByteArray arr (off + 23) (c2w 's')
      PM.writeByteArray arr (off + 24) (c2w '&')
      PM.writeByteArray arr (off + 25) (c2w 'd')
      PM.writeByteArray arr (off + 26) (c2w 'b')
      PM.writeByteArray arr (off + 27) (c2w '=')
      i0 <- copySmall arr (off + 28) db
      PM.copyAddrToByteArray arr i0 staticRequestHeaders staticRequestHeadersLen
      let i1 = i0 + staticRequestHeadersLen
      i2 <- copySmall arr i1 host
      PM.writeByteArray arr i2 (c2w '\r')
      PM.writeByteArray arr (i2 + 1) (c2w '\n')
      PM.writeByteArray arr (i2 + 2) (c2w '\r')
      PM.writeByteArray arr (i2 + 3) (c2w '\n')
      let i3 = i2 + 4
      pure (Just i3)
    else pure Nothing

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

open :: Peer -> IO Influx
open Peer{address,port} = do
  arr <- PM.newByteArray metadataSize
  PM.writeUnalignedByteArray arr 0 (0 :: Word64)
  PM.writeUnalignedByteArray arr 8 ((-1) :: CInt)
  PM.writeUnalignedByteArray arr 16 (getIPv4 address)
  PM.writeUnalignedByteArray arr 20 port
  PM.writeUnalignedByteArray arr 22 (0 :: Word16)
  peerDescr <- fromByteString (IPv4.encodeUtf8 address)
  bufRef <- PM.unsafeNewUnliftedArray 1
  PM.writeUnliftedArray bufRef 0 =<< PM.newByteArray minimumBufferSize
  pure (Influx arr peerDescr bufRef)

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
write !inf@(Influx _ peerDescr bufRef) (Database db) !points0 = getConnection inf >>= \case
  Left err -> pure $! Left $! ConnectException err
  Right conn -> do
    -- TODO: escape the database name correctly
    let host = peerDescr
        bufSz0 = minimumBufferSize
    buf0 <- PM.readUnliftedArray bufRef 0
    MutableByteArrayOffset{array=buf1,offset=i0} <- BB.pasteGrowIO
      4096 (encodeHttpHeaders host db)
      (MutableByteArrayOffset{array=buf0,offset=0})
    -- We reserve 18 bytes for the hexadecimal encoding of a
    -- length that is at most 32 bits and the CRLF after it.
    let i1 = i0 + (16 + 2)
    let go :: Int -- starting offset, zero after first run
           -> Vector Point
           -> MutableBytes RealWorld
           -> IO (MutableByteArray RealWorld, Either InfluxException ())
        go !off0 !points !buf@MutableBytes{array=bufArr} = if V.null points
          then pure (bufArr, Right ())
          else do
            -- This slice into the buffer represents the unused bytes,
            -- not the ones that were successfully written to. This
            -- is a little backwards from what one might imagine. By
            -- taking the offset to be 0 and the length to be the returned
            -- offset, we can get the slice that has been written to.
            (!pointsNext,MutableBytes barrNext ixNext _) <-
              BB.pasteArrayIO buf encodePoint points
            bufSpace <- PM.getSizeofMutableByteArray barrNext
            if V.length pointsNext == V.length points
              then do
                barrNext' <- PM.resizeMutableByteArray barrNext (bufSpace + 4096)
                go off0 pointsNext (MutableBytes barrNext' 0 (bufSpace + (4096 - 2)))
              else do
                -- We take pains to ensure that there are always two
                -- unused bytes at the end of the array. This gives
                -- us the space we need for the CRLF.
                PM.writeByteArray barrNext ixNext (c2w '\r')
                PM.writeByteArray barrNext (ixNext + 1) (c2w '\n')
                -- Now, we go back to the beginning of byte array and write
                -- the hex-encoded length. We do not know the length in advance,
                -- so we write it out after running the builder.
                off <- BBU.pasteIO
                  (BBU.word64PaddedUpperHex (fromIntegral (ixNext - off0 - (16 + 2))))
                  barrNext
                  off0
                PM.writeByteArray barrNext off (c2w '\r')
                PM.writeByteArray barrNext (off + 1) (c2w '\n')
                SMB.send conn (MutableBytes barrNext 0 (ixNext + 2)) >>= \case
                  Left err -> pure (barrNext,Left (SendException err))
                  Right (_ :: ()) -> go 0 pointsNext
                    (MutableBytes barrNext (16 + 2) (bufSpace - (2 + 16 + 2)))
    !len1 <- fmap (\x -> x - i1) (PM.getSizeofMutableByteArray buf1)
    -- Discard the possibly enlarged buffer.
    -- TODO: Store this buffer for reuse.
    go i0 points0 (MutableBytes buf1 i1 (len1 - 2)) >>= \case
      (bufNew, Right (_ :: ())) -> do
        -- It would be really nice if we could avoid making
        -- a syscall just to push out these five additional bytes.
        -- However, there is not a terribly elegant way to do this.
        -- For now, we opt for simplicity.
        PM.writeByteArray bufNew 0 (c2w '0')
        PM.writeByteArray bufNew 1 (c2w '\r')
        PM.writeByteArray bufNew 2 (c2w '\n')
        PM.writeByteArray bufNew 3 (c2w '\r')
        PM.writeByteArray bufNew 4 (c2w '\n')
        SMB.send conn (MutableBytes bufNew 0 5) >>= \case
          Left err -> do
            PM.writeUnliftedArray bufRef 0 bufNew
            writeLocalPort inf 0
            SCK.disconnect_ conn
            pure $! Left $! SendException err
          Right (_ :: ()) -> do
            (bufNew',e) <- receiveResponseStage1 conn bufNew
            PM.writeUnliftedArray bufRef 0 bufNew'
            case e of
              Left err -> pure (Left err)
              Right (_ :: ()) -> pure (Right ())
      (bufNew, Left err) -> do
        PM.writeUnliftedArray bufRef 0 bufNew
        writeLocalPort inf 0
        SCK.disconnect_ conn
        pure (Left err)

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

with :: Peer -> (Influx -> IO a) -> IO (Either SCK.CloseException (), a)
with p f = do
  inf <- open p
  mask $ \restore -> do
    a <- onException (restore (f inf)) (disconnected inf)
    e <- disconnected inf
    pure (e,a)

receiveResponseStage1 ::
     Connection
  -> MutableByteArray RealWorld
  -> IO (MutableByteArray RealWorld, Either InfluxException ())
receiveResponseStage1 conn arr = do
  sz <- PM.getSizeofMutableByteArray arr
  SMB.receiveBetween conn (MutableBytes arr 0 sz) 12 >>= \case
    Left err -> pure $! (arr, Left $! ReceiveException $! err)
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
        else pure $! (arr, Left ResponseException)

-- Precondition: ix >= 4
receiveResponseStage2 ::
     Connection
  -> MutableByteArray RealWorld
  -> Int -- Index
  -> Int -- Total size
  -> IO (MutableByteArray RealWorld, Either InfluxException ())
receiveResponseStage2 !sock !buffer !ix !sz = if ix < sz
  then do
    (w4 :: Word8) <- PM.readByteArray buffer (ix - 1)
    (w3 :: Word8) <- PM.readByteArray buffer (ix - 2)
    (w2 :: Word8) <- PM.readByteArray buffer (ix - 3)
    (w1 :: Word8) <- PM.readByteArray buffer (ix - 4)
    if w1 == 13 && w2 == 10 && w3 == 13 && w4 == 10
      then pure $! (buffer, Right ())
      else do
        let remaining = sz - ix
        SMB.receiveOnce sock (MutableBytes buffer 0 remaining) >>= \case
          Left err -> pure $! (buffer, Left $! ReceiveException $! err)
          Right n -> receiveResponseStage2 sock buffer (ix + n) sz
  else do
    buffer' <- PM.resizeMutableByteArray buffer (sz * 2)
    receiveResponseStage2 sock buffer' ix (sz * 2)
