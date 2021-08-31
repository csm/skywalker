(ns skywalker.core.server
  (:require [clojure.core.async :as async]
            [clojure.java.nio :as nio]
            [cognitect.anomalies :as anomalies]
            [skywalker.core :as core]
            [skywalker.core.impl :refer :all]
            [clojure.spec.alpha :as s]
            [msgpack.core :as msgpack])
  (:import (java.nio.channels AsynchronousServerSocketChannel)
           (java.nio ByteBuffer)
           (java.security SecureRandom)))

(defn- send-reply
  [socket reply lock]
  (let [bytes (msgpack/pack reply)
        buffer (ByteBuffer/allocate (+ (alength bytes) 2))]
    (.putShort buffer (short (alength bytes)))
    (.put buffer ^bytes bytes)
    (.flip buffer)
    (async/take!
      (write-fully socket buffer lock)
      (fn [res]
        (tap> {:task ::send-reply :result res})
        (when (s/valid? ::anomalies/anomaly res)
          (println "todo, handle errors!" (prn res)))))))

(defn handler
  [junction socket tokens]
  (async/go-loop []
    (let [message (async/<! (read-message socket))
          lock (doto (async/chan 1) (async/put! true))]
      (if (s/valid? ::anomalies/anomaly message)
        message
        (let [message (try
                        (msgpack/unpack-stream (data-input message))
                        (catch Exception e
                          {::anomalies/category ::anomalies/incorrect
                           ::anomalies/message (.getMessage e)
                           ::cause e}))]
          (tap> {:task ::handler :phase :read-message :message message})
          (case (first message)
            ":send!" (let [[_ msgid timeout timeout-val id value] message]
                       (tap> {:task ::handler :phase :begin-send :message message})
                       (async/take!
                         (core/send! junction id value {:timeout timeout :timeout-val timeout-val})
                         (fn [result]
                           (tap> {:task ::handler :phase :send-result :result result})
                           (send-reply socket [":send!" msgid result] lock)))
                       (recur))
            ":recv!" (let [[_ msgid timeout timeout-val id] message]
                       (tap> {:task ::handler :phase :begin-recv :message message})
                       (async/take!
                         (core/recv! junction id {:timeout timeout :timeout-val timeout-val})
                         (fn [result]
                           (tap> {:task ::handler :phase :recv-result :result result})
                           (send-reply socket [":recv!" msgid result] lock)))
                       (recur))
            ":tokens" (let [[_ msgid] message]
                        (tap> {:task ::handler :phase :send-tokens :message message})
                        (send-reply socket [":tokens" msgid tokens]))
            (println "invalid message:" (pr-str message))))))))

(defn server
  [bind-address & {:keys [backlog tokens num-tokens] :or {backlog 0 num-tokens 32}}]
  (let [server (AsynchronousServerSocketChannel/open)
        junction (core/local-junction)
        random (when-not tokens (SecureRandom.))
        tokens (or tokens (->> (range num-tokens)
                               (map #(.nextLong random))
                               (sort)
                               (into [])))]
    (.bind server bind-address backlog)
    (let [server-chan (async/go-loop []
                        (let [socket (async/<! (nio/accept server {}))]
                          (tap> {:task ::server :phase :accepted-socket :socket socket})
                          (if (s/valid? ::anomalies/anomaly socket)
                            (do
                              (.close server)
                              server)
                            (do
                              (async/go
                                (async/<! (handler junction socket tokens))
                                (.close socket))
                              (recur)))))]
      {:socket server
       :server-chan server-chan})))
