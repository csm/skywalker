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

(deftype RemoteJunction [msgid-atom socket method-calls lock]
  core/Junction
  (send! [_ id value opts]
    (let [{:keys [timeout timeout-val] :or {timeout 60000 timeout-val :skywalker/timeout}} opts]
      (send-msg-common socket msgid-atom method-calls lock ":send!" timeout timeout-val id value)))

  (recv! [_ id opts]
    (let [{:keys [timeout timeout-val] :or {timeout 60000 timeout-val :skywalker/timeout}} opts]
      (send-msg-common socket msgid-atom method-calls lock ":recv!" timeout timeout-val id)))

  Client
  (start! [this]
    (async/go-loop []
      (let [message (async/<! (read-message socket))]
        (tap> {:task ::start! :phase :read-message :message message})
        (if (s/valid? ::anomalies/anomaly message)
          (do
            (tap> {:task ::run! :phase :errored :anomaly message})
            (.close this))
          (let [result (try
                         (let [message (msgpack/unpack-stream (data-input message))
                               [_ msgid result] message]
                           (tap> {:task ::start! :phase :parsed-message :message message})
                           (if-let [receiver (.get method-calls ^long (long msgid))]
                             (async/put! receiver result)
                             (println "error, invalid message ID" msgid "method calls:" method-calls)))
                         (catch Exception e
                           {::anomalies/category ::anomalies/fault
                            ::anomalies/message (.getMessage e)
                            ::cause e}))]
            (if (s/valid? ::anomalies/anomaly result)
              (tap> {:task ::run! :phase :error-reading-message :anomaly result})
              (recur)))))))

  Closeable
  (close [_]
    (doseq [ch (.values method-calls)]
      (async/close! ch))
    (.close socket)))

(defn remote-junction
  [address opts]
  (async/go
    (let [socket (AsynchronousSocketChannel/open)
          result (async/<! (nio/connect socket address opts))]
      (tap> {:task ::remote-junction :phase :connected :result result})
      (if (s/valid? ::anomalies/anomaly result)
        result
        (let [junct (->RemoteJunction (atom 0) socket (NonBlockingHashMapLong.) (doto (async/chan 1)
                                                                                  (async/put! true)))]
          (start! junct)
          junct)))))
