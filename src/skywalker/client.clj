(ns skywalker.client
  (:require [clojure.core.async :as async]
            [clojure.java.nio :as nio]
            [clojure.spec.alpha :as s]
            [cognitect.anomalies :as anomalies]
            [msgpack.core :as msgpack]
            [skywalker.core :as core]
            [skywalker.core.impl :refer :all]
            [skywalker.taplog :as log])
  (:import (org.cliffc.high_scale_lib NonBlockingHashMapLong)
           (java.nio.channels AsynchronousSocketChannel)
           (java.io Closeable)
           (java.util UUID)
           (java.net InetSocketAddress)
           (skywalker.core.impl Timeout)))

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
    (log/log :debug {:task ::ensure-connection
                     :phase :begin
                     :socket socket
                     :remote-address remote-address})
    (let [s @socket]
      (if (nil? s)
        (async-locking lock
          (let [news (AsynchronousSocketChannel/open)
                connect (async/<! (nio/connect news remote-address {}))]
            (log/log :debug {:task ::ensure-connection
                             :phase :connected
                             :result connect})
            (if (s/valid? ::anomalies/anomaly connect)
              connect
              (do
                (compare-and-set! socket s news)
                (async/put! socket-chan true)
                news))))
        s))))

(defn- send-message-with-retry
  [socket socket-chan remote-address retries max-retry msgid-atom method-calls write-lock method & args]
  (async/go-loop [tries retries delay 8]
    (log/log :debug {:task ::send-message-with-retry
                     :phase :begin
                     :tries tries :delay delay})
    (let [socket (async/<! (ensure-connection socket socket-chan remote-address write-lock))]
      (log/log :debug {:task ::send-message-with-retry
                       :phase :got-socket
                       :socket socket})
      (if (s/valid? ::anomalies/anomaly socket)
        (if (pos? tries)
          (do
            (async/<! (async/timeout delay))
            (recur (dec tries) (min max-retry (* delay 2))))
          socket)
        (let [result (async/<! (apply send-msg-common socket msgid-atom method-calls write-lock method args))]
          (if (s/valid? ::anomalies/anomaly result)
            (if (pos? tries)
              (do
                (reset! socket nil)
                (async/<! (async/timeout delay))
                (recur (dec tries) (min max-retry (* delay 2))))
              result)
            result))))))

(deftype RemoteJunction [msgid-atom socket remote-address retries max-delay method-calls read-lock write-lock socket-chan junction-id node]
  core/Junction
  (send! [_ id value opts]
    (let [{:keys [timeout timeout-val] :or {timeout 60000 timeout-val :skywalker/timeout}} opts]
      (async/go-loop [timeout timeout]
        (let [start (System/currentTimeMillis)
              result (async/<! (send-message-with-retry socket socket-chan remote-address retries max-delay msgid-atom method-calls write-lock ":send!" timeout id value))
              elapsed (- (System/currentTimeMillis) start)]
          (log/log :debug {:task ::send! :phase :got-result :result result :elapsed elapsed})
          (cond
            (= result ::closed)
            (if (pos? (- timeout elapsed))
              (recur (- timeout elapsed))
              timeout-val)

            (satisfies? ITimeout result) timeout-val

            :else result)))))

  (recv! [_ id opts]
    (let [{:keys [timeout timeout-val] :or {timeout 60000 timeout-val :skywalker/timeout}} opts]
      (async/go-loop [timeout timeout]
        (let [start (System/currentTimeMillis)
              result (async/<! (send-message-with-retry socket socket-chan remote-address retries max-delay msgid-atom method-calls write-lock ":recv!" timeout id))
              elapsed (- (System/currentTimeMillis) start)]
          (log/log :debug {:task ::recv! :phase :got-result :result result :elapsed elapsed})
          (cond
            (= result ::closed)
            (if (pos? (- timeout elapsed))
              (recur (- timeout elapsed))
              timeout-val)

            (satisfies? ITimeout result) timeout-val

            :else result)))))

  Client
  (start! [this]
    (async/go-loop []
      (log/log :debug {:task ::start!
                       :phase :begin})
      (if-let [s @socket]
        (let [_ (log/log :debug {:task ::start!
                                 :phase :got-socket
                                 :socket s})
              message (async/<! (read-message s read-lock))]
          (log/log :debug {:task ::start! :phase :read-message :message message})
          (if (s/valid? ::anomalies/anomaly message)
            (do
              (async-locking write-lock
                (log/log :debug {:task ::start :phase :errored :anomaly message})
                (compare-and-set! socket s nil)
                (doseq [ch (.values method-calls)]
                  (async/put! ch ::closed))
                (.clear method-calls))
              (recur))
            (let [result (try
                           (let [message (msgpack/unpack-stream (data-input message))
                                 [_ msgid result] message]
                             (log/log :debug {:task ::start! :phase :parsed-message :message message})
                             (if-let [receiver (.remove method-calls ^long (long msgid))]
                               (async/put! receiver result)
                               (log/log :error {:task ::start!
                                                :phase :handling-response
                                                :error :bad-message-id
                                                :message-id msgid})))
                           (catch Exception e
                             (log/log :error {:task ::start!
                                              :phase :caught-exception-handling-reply
                                              :exception e})
                             {::anomalies/category ::anomalies/fault
                              ::anomalies/message (.getMessage e)
                              ::cause e}))]
              (if (s/valid? ::anomalies/anomaly result)
                (log/log :debug {:task ::run! :phase :error-reading-message :anomaly result})
                (recur)))))
        (do
          (async/alts! [socket-chan (async/timeout 1000)])
          (recur)))))

  Tokened
  (tokens [_ _opts]
    (send-message-with-retry socket socket-chan remote-address retries max-delay msgid-atom method-calls write-lock ":tokens"))

  Closeable
  (close [_]
    (doseq [ch (.values method-calls)]
      (async/close! ch))
    (when-let [s @socket] (.close s))))

(defmethod print-method RemoteJunction
  [j w]
  (.write w (pr-str {:type 'skywalker.client/RemoteJunction
                     :msgid (deref (.-msgid_atom j))
                     :connected? (some-> (.-socket j) ^AsynchronousSocketChannel (deref) (.isOpen))
                     :remote-address {:host (.getHostAddress (.getAddress ^InetSocketAddress (.-remote_address j)))
                                      :port (.getPort ^InetSocketAddress (.-remote_address j))}
                     :id (.-junction_id j)
                     :node (.-node j)})))

(defn remote-junction
  [address opts]
  (async/go
    (let [socket (AsynchronousSocketChannel/open)
          result (async/<! (nio/connect socket address opts))]
      (log/log :debug {:task ::remote-junction :phase :connected :result result})
      (if (s/valid? ::anomalies/anomaly result)
        result
        (let [junct (->RemoteJunction (atom 0)
                                      (atom socket)
                                      address
                                      (get opts :retries 10)
                                      (get opts :max-delay 60000)
                                      (NonBlockingHashMapLong.)
                                      (async-lock)
                                      (async-lock)
                                      (async/chan)
                                      (:id opts (str (UUID/randomUUID)))
                                      (:node opts (str (UUID/randomUUID))))]
          (start! junct)
          junct)))))
