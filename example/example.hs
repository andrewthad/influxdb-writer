{-# language LambdaCase #-}
{-# language NamedFieldPuns #-}
{-# language TypeApplications #-}
{-# language PackageImports #-}

import "primitive" Data.Primitive (Array,ByteArray)
import Data.Char (ord)
import Data.Vector (Vector)
import Database.Influx.LineProtocol
import Database.Influx.Write
import Socket.Stream.IPv4 (Peer(..))
import Data.Word (Word8)
import Net.Types (IPv4)
import System.Environment (getArgs)
import qualified Data.Vector as V
import qualified GHC.Exts as Exts
import qualified Net.IPv4 as IPv4
import qualified Data.ByteString.Char8 as BC8
import qualified Data.Bytes as Bytes

import qualified "primitive" Data.Primitive as PM

main :: IO ()
main = getArgs >>= \case
  [host] -> do
    address <- parseAddr host
    putStrLn "Starting without BasicAuth"
    (e,()) <- with (Peer{address,port=8086}) $ \inf -> do
      write inf (Database (str "my_database")) points >>= \case
        Left err -> fail ("write failed: " ++ show err)
        Right () -> pure ()
    case e of
      Left _ -> fail "close failed"
      Right () -> pure ()
    putStrLn "Finished"
  [host,user,pass] -> do
    address <- parseAddr host
    let user' = Bytes.fromLatinString user
    let pass' = Bytes.fromLatinString pass
    putStrLn "Starting with BasicAuth"
    (e,()) <- withBasicAuth (Peer{address,port=8086}) user' pass' $ \inf -> do
      write inf (Database (str "my_database")) points >>= \case
        Left err -> fail ("write failed: " ++ show err)
        Right () -> pure ()
    case e of
      Left _ -> fail "close failed"
      Right () -> pure ()
    putStrLn "Finished"
  _ -> fail "Expected either 0 or 2 arguments" 

parseAddr :: String -> IO IPv4
parseAddr host = case IPv4.decodeUtf8 (BC8.pack host) of
  Nothing -> fail "Could not parse IPv4 address"
  Just addr -> pure addr

egressIngress :: FieldValue -> FieldValue -> Fields
egressIngress = fields2
  (fieldKeyByteArray (str "egress"))
  (fieldKeyByteArray (str "ingress"))

color :: TagValue -> Tags
color = tags1 (tagKeyByteArray (str "color"))

colors :: Array TagValue
colors = Exts.fromList
  [ tagValueByteArray (str "green")
  , tagValueByteArray (str "red")
  , tagValueByteArray (str "blue")
  , tagValueByteArray (str "orange")
  , tagValueByteArray (str "violet")
  , tagValueByteArray (str "yellow")
  , tagValueByteArray (str "indigo")
  ]

rocks :: Measurement
rocks = measurementByteArray (str "rocks")

makePoint :: Int -> Point
makePoint i = Point
  { measurement = rocks
  , tags = color (PM.indexArray colors (mod i (PM.sizeofArray colors)))
  , fields = egressIngress
      (fieldValueWord64 (fromIntegral (div (i * i) 100)))
      (fieldValueWord64 (fromIntegral i))
  , time = 1442365427643 + (fromIntegral i * 10000000)
  }

points :: Vector Point
points = V.generate 40000 makePoint

str :: String -> ByteArray
str = Exts.fromList . map (fromIntegral @Int @Word8 . ord)
