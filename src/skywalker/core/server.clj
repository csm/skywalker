(ns skywalker.core.server
  (:require [clojure.core.async :as async]
            [clojure.java.nio :as nio]
            [cognitect.anomalies :as anomalies]
            [skywalker.core :as core]
            [skywalker.core.impl :refer :all]
            [clojure.spec.alpha :as s]
            [msgpack.core :as msgpack])
  (:import (java.nio.channels AsynchronousServerSocketChannel)
           (java.nio ByteBuffer)))

(defn- send-reply
  [socket reply]
  (let [bytes (msgpack/pack reply)
        buffer (ByteBuffer/allocate (+ (alength bytes) 2))]
    (.putShort buffer (short (alength bytes)))
    (.put buffer ^bytes bytes)
    (async/take!
      (write-fully socket buffer)
      (fn [res] (when (s/valid? ::anomalies/anomaly res)
                  (println "todo, handle errors!" (prn res)))))))

(defn handler
  [junction socket]
  (async/go-loop []
    (let [message (async/<! (read-message socket))]
      (if (s/valid? ::anomalies/anomaly message)
        message
        (let [message (try
                        (msgpack/unpack-stream (data-input message))
                        (catch Exception e
                          {::anomalies/category ::anomalies/incorrect
                           ::anomalies/message (.getMessage e)
                           ::cause e}))]
          (case (first message)
            ":send!" (let [[_ msgid timeout timeout-val id value] message]
                       (async/take!
                         (core/send! junction id value {:timeout timeout :timeout-val timeout-val})
                         (fn [result]
                           (send-reply socket [":send!" msgid result]))))
            ":recv!" (let [[_ msgid timeout timeout-val id] message]
                       (async/take!
                         (core/recv! junction id {:timeout timeout :timeout-val timeout-val})
                         (fn [result]
                           (send-reply socket [":recv!" msgid result]))))
            (println "invalid message:" (pr-str message))))))))

(defn server
  [bind-address & {:keys [backlog] :or {backlog 0}}]
  (let [server (AsynchronousServerSocketChannel/open)
        junction (core/local-junction)]
    (.bind server bind-address backlog)
    (async/go-loop []
      (let [socket (async/<! (nio/accept server {}))]
        (if (s/valid? ::anomalies/anomaly socket)
          (do
            (.close server)
            server)
          (do
            (async/go
              (async/<! (handler junction socket))
              (.close socket))
            (recur)))))))
