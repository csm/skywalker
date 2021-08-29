(ns skywalker.core.impl
  "Common implementation details for client/server."
  (:require [clojure.core.async :as async]
            [clojure.java.nio :as nio]
            [clojure.spec.alpha :as s]
            [cognitect.anomalies :as anomalies]
            [msgpack.core :as msgpack]
            msgpack.clojure-extensions)
  (:import (java.io DataInput IOException)
           (java.nio ByteBuffer BufferUnderflowException)
           (java.nio.charset StandardCharsets)))

(defn write-fully
  [socket buffer lock]
  (async/go
    (async/<! lock)
    (loop []
      (if (.hasRemaining buffer)
        (do
          (tap> {:task ::write-fully :remaining (.remaining buffer)})
          (let [result (async/<! (nio/write socket buffer {}))]
            (if (s/valid? ::anomalies/anomaly result)
              (do
                (async/put! lock true)
                result)
              (recur))))
        (do
          (async/put! lock true)
          nil)))))

(defn send-msg-common
  [socket msgid-atom method-calls lock method & args]
  (async/go
    (let [msgid (swap! msgid-atom inc)
          msg (into [method msgid] args)
          _ (tap> {:task ::send-msg-common :msg msg})
          bytes (msgpack/pack msg)
          buffer (ByteBuffer/allocate (+ (alength bytes) 2))
          result-chan (async/promise-chan)]
      (.put method-calls msgid result-chan)
      (.putShort buffer (short (alength bytes)))
      (.put buffer ^bytes bytes)
      (.flip buffer)
      (let [result (async/<! (write-fully socket buffer lock))]
        (if (s/valid? ::anomalies/anomaly result)
          result)
        (async/<! result-chan)))))

(defn read-fully
  [socket length]
  (async/go-loop [buffer (ByteBuffer/allocate length)]
    (tap> {:task ::read-fully :buffer buffer :socket socket})
    (if (.hasRemaining buffer)
      (let [result (async/<! (nio/read socket {:buffer buffer}))]
        (tap> {:task ::read-fully :read-result result})
        (if (s/valid? ::anomalies/anomaly result)
          result
          (if (neg? (:length result))
            (if (.hasRemaining buffer)
              {::anomalies/category ::anomalies/interrupted
               ::anomalies/message "Socket closed, but awaiting more bytes"}
              (.flip buffer))
            (recur buffer))))
      (.flip buffer))))

(defn read-message
  [socket]
  (async/go
    (let [len-buf (async/<! (read-fully socket 2))]
      (tap> {:task ::read-message :len-buf len-buf})
      (if (s/valid? ::anomalies/anomaly len-buf)
        len-buf
        (let [length (bit-and (.getShort len-buf 0) 0xFFFF)]
          (tap> {:task ::read-message :length length})
          (if (> length 16384)
            {::anomalies/category ::anomalies/incorrect
             ::anomalies/message (str "message length " length " too long")}
            (async/<! (read-fully socket length))))))))

(defmacro buffer->io
  [& forms]
  `(try
     ~@forms
     (catch BufferUnderflowException e#
       (throw (IOException. ^Throwable e#)))))

(defn data-input
  [^ByteBuffer buffer]
  (reify DataInput
    (^void readFully [_ ^bytes bytes]
      (buffer->io (.get buffer bytes)))
    (^void readFully [_ ^bytes bytes ^int offset ^int length]
      (buffer->io (.get buffer bytes offset length)))
    (skipBytes [_ n]
      (buffer->io (.position buffer (+ (.position buffer) n))))
    (readBoolean [_]
      (buffer->io (not= 0 (.get buffer))))
    (readByte [_]
      (buffer->io (.get buffer)))
    (readUnsignedByte [_]
      (buffer->io (bit-and (.get buffer) 0xFF)))
    (readShort [_]
      (buffer->io (.getShort buffer)))
    (readUnsignedShort [_]
      (buffer->io (bit-and (.getShort buffer) 0xFFFF)))
    (readChar [_]
      (buffer->io (char (bit-and (.getShort buffer) 0xFFFF))))
    (readInt [_]
      (buffer->io (.getInt buffer)))
    (readLong [_]
      (buffer->io (.getLong buffer)))
    (readFloat [_]
      (buffer->io (.getFloat buffer)))
    (readDouble [_]
      (buffer->io (.getDouble buffer)))
    (readLine [_]
      (buffer->io
        (loop [buf (StringBuilder.)]
          (let [ch (bit-and (.get buffer) 0xFF)]
            (if (= ch (int \newline))
              (.toString buf)
              (recur (.append buf (char ch))))))))
    (readUTF [_]
      (buffer->io
        (let [len (bit-and (.getShort buffer) 0xFFFF)
              buf (byte-array len)]
          (.get buffer buf)
          (String. buf StandardCharsets/UTF_8))))))

(defprotocol AsyncLock
  (lock [this])
  (unlock [this]))

(defn async-lock
  []
  (let [chan (async/chan 1)]
    (async/put! chan true)
    (reify AsyncLock
      (lock [_]
        (async/go (async/<! chan)))
      (unlock [_]
        (async/put! chan true)))))