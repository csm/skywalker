(ns skywalker.server
  (:require [clojure.core.async :as async]
            [clojure.java.nio :as nio]
            [cognitect.anomalies :as anomalies]
            [skywalker.core :as core]
            [skywalker.core.impl :refer :all]
            [clojure.spec.alpha :as s]
            [msgpack.core :as msgpack]
            [skywalker.cluster.client :as cc]
            [skywalker.taplog :as log])
  (:import (java.nio.channels AsynchronousServerSocketChannel)
           (java.nio ByteBuffer)
           (java.security SecureRandom)))

(defn- send-reply
  [socket reply lock]
  (log/log :trace {:task ::send-reply :phase :begin :reply reply})
  (let [bytes (msgpack/pack reply)
        buffer (ByteBuffer/allocate (+ (alength bytes) 2))]
    (.putShort buffer (short (alength bytes)))
    (.put buffer ^bytes bytes)
    (.flip buffer)
    (async/go
      (let [res (async/<! (write-fully socket buffer lock))]
        (log/log :trace {:task ::send-reply :phase :end :result res})
        (when (s/valid? ::anomalies/anomaly res)
          res)))))

(defn handler
  [junction socket tokens read-lock write-lock]
  (log/log :debug {:task ::handler
                   :phase :begin
                   :junction junction
                   :socket socket
                   :tokens tokens})
  (async/go-loop []
    (log/log :debug {:task ::handler :phase :begin-loop :socket socket})
    (let [message (async/<! (read-message socket read-lock))]
      (if (s/valid? ::anomalies/anomaly message)
        message
        (let [message (try
                        (msgpack/unpack-stream (data-input message))
                        (catch Exception e
                          {::anomalies/category ::anomalies/incorrect
                           ::anomalies/message (.getMessage e)
                           ::cause e}))]
          (log/log :debug {:task ::handler :phase :read-message :message message})
          (case (first message)
            ":send!" (let [[_ msgid timeout id value] message]
                       (log/log :debug {:task ::handler :phase :begin-send :message message})
                       (async/take!
                         (core/send! junction id value {:timeout timeout :timeout-val (->Timeout)})
                         (fn [result]
                           (log/log :debug {:task ::handler :phase :send-result :result result})
                           (async/take!
                             (send-reply socket [":send!" msgid result]
                                         write-lock)
                             (fn [res]
                               (log/log :debug {:task ::handler :phase :sent-send-reply :result res})))))
                       (recur))
            ":recv!" (let [[_ msgid timeout id] message]
                       (log/log :debug {:task ::handler :phase :begin-recv :message message})
                       (async/take!
                         (core/recv! junction id {:timeout timeout :timeout-val (->Timeout)})
                         (fn [result]
                           (log/log :debug {:task ::handler :phase :recv-result :result result})
                           (async/take!
                             (send-reply socket [":recv!" msgid result] write-lock)
                             (fn [res]
                               (log/log :debug {:task ::handler :phase :sent-recv-reply :result res})))))
                       (recur))
            ":tokens" (let [[_ msgid] message]
                        (log/log :debug {:task ::handler :phase :send-tokens :message message})
                        (async/take!
                          (send-reply socket [":tokens" msgid tokens] write-lock)
                          (fn [res]
                            (log/log :debug {:task ::handler :phase :send-tokens-reply :result res})))
                        (recur))
            (println "invalid message:" (pr-str message))))))))

(defn server
  "Start a skywalker server, bound to bind-address.

  Options include:

  - :backlog - The server socket backlog, default 0.
  - :tokens - An explicit set of tokens (random long integers). If omitted random tokens are generated.
  - :num-tokens - The number of random tokens to generate, default 32.

  Returns a map with values:

  - :server - The underlying AsynchronousServerSocketChannel.
  - :server-chan - A channel that will yield when the server accept loop
    stops.
  - :closer-chan - A channel that you can use to close active connections.
    Pass a predicate function that will receive the AsynchronousSocketChannel
    and return a truthy value if the socket should be closed."
  [bind-address & {:keys [backlog tokens num-tokens discovery] :or {backlog 0 num-tokens 32}}]
  (let [server (AsynchronousServerSocketChannel/open)
        junction (core/local-junction)
        random (when-not tokens (SecureRandom.))
        tokens (or tokens (->> (range num-tokens)
                               (map (fn [_] (.nextLong random)))
                               (sort)
                               (into [])))
        junction (if discovery
                   (cc/cluster-client discovery {:tokens tokens
                                                 :junction junction})
                   junction)
        closer (async/chan)
        connections (atom #{})]
    (.bind server bind-address backlog)
    (async/go-loop []
      (when-let [pred (async/<! closer)]
        (doseq [socket @connections]
          (when (pred socket)
            (try
              (.close socket)
              (catch Exception _))))))
    (let [server-chan (async/go-loop []
                        (let [socket (async/<! (nio/accept server {}))]
                          (log/log :debug {:task ::server :phase :accepted-socket :socket socket})
                          (if (s/valid? ::anomalies/anomaly socket)
                            (do
                              (.close server)
                              (async/close! closer)
                              server)
                            (do
                              (async/go
                                (swap! connections conj socket)
                                (async/<! (handler junction socket tokens (async-lock) (async-lock)))
                                (try
                                  (.close socket)
                                  (catch Exception _))
                                (swap! connections disj socket))
                              (recur)))))]
      {:socket server
       :server-chan server-chan
       :closer-chan closer})))
