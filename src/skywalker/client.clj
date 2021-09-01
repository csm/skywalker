(ns skywalker.client
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

(defprotocol Tokened
  (tokens [this opts]
    "Fetch the set of tokens for this node.

    Returns a channel that will yield a vector of this node's
    tokens on success, or an anomaly on failure.

    There are no options at this time."))

(defn- ensure-connection
  [socket socket-chan remote-address lock]
  (async/go-loop []
    (let [s @socket]
      (if (nil? s)
        (do
          (async/<! lock)
          (let [news (AsynchronousSocketChannel/open)
                connect (async/<! (nio/connect news remote-address {}))]
            (if (s/valid? ::anomalies/anomaly connect)
              (do
                (async/put! lock true)
                connect)
              (do
                (compare-and-set! socket s news)
                (async/put! lock true)
                (async/put! socket-chan true)
                news))))
        s))))

(defn- send-message-with-retry
  [socket socket-chan remote-address retries max-retry msgid-atom method-calls lock method & args]
  (async/go-loop [tries retries delay 8]
    (let [socket (async/<! (ensure-connection socket socket-chan remote-address lock))]
      (if (s/valid? ::anomalies/anomaly socket)
        (if (pos? tries)
          (do
            (async/<! (async/timeout delay))
            (recur (dec tries) (min max-retry (* delay 2))))
          socket)
        (let [result (async/<! (apply send-msg-common socket msgid-atom method-calls lock method args))]
          (if (s/valid? ::anomalies/anomaly result)
            (if (pos? tries)
              (do
                (reset! socket nil)
                (async/<! (async/timeout delay))
                (recur (dec tries) (min max-retry (* delay 2))))
              result)
            result))))))

(deftype RemoteJunction [msgid-atom socket remote-address retries max-delay method-calls lock socket-chan]
  core/Junction
  (send! [_ id value opts]
    (let [{:keys [timeout timeout-val] :or {timeout 60000 timeout-val :skywalker/timeout}} opts]
      (send-message-with-retry socket socket-chan remote-address retries max-delay msgid-atom method-calls lock ":send!" timeout timeout-val id value)))

  (recv! [_ id opts]
    (let [{:keys [timeout timeout-val] :or {timeout 60000 timeout-val :skywalker/timeout}} opts]
      (send-message-with-retry socket socket-chan remote-address retries max-delay msgid-atom method-calls lock ":recv!" timeout timeout-val id)))

  Client
  (start! [this]
    (async/go-loop []
      (if-let [s @socket]
        (let [message (async/<! (read-message s))]
          (tap> {:task ::start! :phase :read-message :message message})
          (if (s/valid? ::anomalies/anomaly message)
            (do
              (tap> {:task ::run! :phase :errored :anomaly message})
              (async/<! lock)
              (compare-and-set! socket s nil)
              (async/put! lock true)
              (recur))
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
                (recur)))))
        (do
          (async/alts! [socket-chan (async/timeout 1000)])
          (recur)))))

  Tokened
  (tokens [_ _opts]
    (send-message-with-retry socket socket-chan remote-address retries max-delay msgid-atom method-calls lock ":tokens"))

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
        (let [junct (->RemoteJunction (atom 0)
                                      (atom socket)
                                      address
                                      (get opts :retries 10)
                                      (get opts :max-delay 60000)
                                      (NonBlockingHashMapLong.)
                                      (doto (async/chan 1)
                                        (async/put! true))
                                      (async/chan))]
          (start! junct)
          junct)))))
