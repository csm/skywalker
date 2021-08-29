(ns skywalker.core.impl
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
  [socket buffer]
  (async/go-loop []
    (when (.hasRemaining buffer)
      (let [result (async/<! (nio/write socket buffer {}))]
        (if (s/valid? ::anomalies/anomaly result)
          result
          (recur))))))

(defn send-msg-common
  [socket msgid-atom method-calls method & args]
  (async/go
    (let [msgid (swap! msgid-atom inc)
          msg (into [method msgid] args)
          bytes (msgpack/pack msg)
          buffer (ByteBuffer/allocate (+ (alength bytes) 2))
          result (async/promise-chan)]
      (.put method-calls msgid result)
      (.putShort buffer (short (alength bytes)))
      (.put buffer ^bytes bytes)
      (let [result (async/<! (write-fully socket buffer))]
        (if (s/valid? ::anomalies/anomaly result)
          result)
        (async/<! result)))))

(defn read-fully
  [socket length]
  (async/go-loop [buffer (ByteBuffer/allocate length)]
    (tap> {:task ::read-fully :buffer buffer})
    (if (.hasRemaining buffer)
      (let [result (async/<! (nio/read socket {:buffer buffer}))]
        (tap> {:task ::read-fully :read-result result})
        (if (s/valid? ::anomalies/anomaly result)
          result
          (recur buffer)))
      (.flip buffer))))

(defn read-message
  [socket]
  (async/go
    (let [len-buf (async/<! (read-fully socket 2))]
      (tap> {:task ::read-message :len-buf len-buf})
      (if (s/valid? ::anomalies/anomaly len-buf)
        len-buf
        (let [length (.getShort len-buf 0)]
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