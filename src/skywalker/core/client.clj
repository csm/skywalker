(ns skywalker.core.client
  (:require [clojure.core.async :as async]
            [clojure.java.nio :as nio]
            [clojure.spec.alpha :as s]
            [cognitect.anomalies :as anomalies]
            [msgpack.core :as msgpack]
            [skywalker.core :as core]
            [skywalker.core.impl :refer :all])
  (:import (org.cliffc.high_scale_lib NonBlockingHashMapLong)
           (java.nio.channels AsynchronousSocketChannel)
           (java.io Closeable)))

(defprotocol Client
  (start! [this] "Starts the client."))

(deftype RemoteJunction [msgid-atom socket method-calls]
  core/Junction
  (send! [_ id value opts]
    (let [{:keys [timeout timeout-val] :or {timeout 60000 timeout-val :skywalker/timeout}} opts]
      (send-msg-common socket msgid-atom method-calls ":send!" timeout timeout-val id value)))

  (recv! [_ id opts]
    (let [{:keys [timeout timeout-val] :or {timeout 60000 timeout-val :skywalker/timeout}} opts]
      (send-msg-common socket msgid-atom method-calls ":recv!" timeout timeout-val id)))

  Client
  (start! [_]
    (async/go-loop []
      (let [message (async/<! (read-message socket))]
        (if (s/valid? ::anomalies/anomaly message)
          "todo, halt and catch fire"
          (let [result (try
                         (let [message (msgpack/unpack-stream (data-input message))
                               [_ msgid result] message]
                           (if-let [receiver (.get method-calls msgid)]
                             (async/put! receiver result)
                             (println "error, invalid message ID" msgid)))
                         (catch Exception e
                           {::anomalies/category ::anomalies/fault
                            ::anomalies/message (.getMessage e)
                            ::cause e}))]
            (if (s/valid? ::anomalies/anomaly result)
              "todo, halt and catch fire"
              (recur)))))))

  Closeable
  (close [_] (.close socket)))

(defn remote-junction
  [address opts]
  (async/go
    (let [socket (AsynchronousSocketChannel/open)
          result (async/<! (nio/connect socket address opts))]
      (if (s/valid? ::anomalies/anomaly result)
        result
        (let [junct (->RemoteJunction (atom 0) result (NonBlockingHashMapLong.))]
          (start! junct)
          junct)))))
