(ns skywalker.remote-test
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [clojure.test :refer :all]
            [cognitect.anomalies :as anomalies]
            [skywalker.client :as client]
            [skywalker.core :as core]
            [skywalker.server :as server])
  (:import (java.net InetSocketAddress InetAddress)))

(comment
  (add-tap (comp println pr-str)))

(def ^:dynamic *port*)
(def ^:dynamic *server*)

(use-fixtures :each
  (fn [f]
    (let [server (server/server (InetSocketAddress. (InetAddress/getLocalHost) 0))
          port (.getPort (.getLocalAddress (:socket server)))]
      (binding [*server* server
                *port* port]
        (f))
      (.close (:socket server)))))

(deftest test-get-tokens
  (let [client1 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))
        tokens (async/<!! (client/tokens client1 {}))]
    (is (not (s/valid? ::anomalies/anomaly tokens)))
    (is (sequential? tokens))
    (is (every? integer? tokens))))

(deftest test-send-timeout
  (let [client1 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))]
    (testing "send timeout"
      (let [result (async/<!! (core/send! client1 "foo" "bar" {:timeout 1000 :timeout-val ::timeout}))]
        (is (= result ::timeout))))))

(deftest test-recv-timeout
  (let [client1 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))]
    (testing "recv timeout"
      (let [result (async/<!! (core/recv! client1 "foo" {:timeout 1000 :timeout-val ::timeout}))]
        (is (= result ::timeout))))))

(deftest test-send-recv
  (let [client1 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))
        client2 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))]
    (testing "send then recv"
      (let [send (core/send! client1 "foo" "bar" {:timeout 1000})
            recv (core/recv! client2 "foo" {:timeout 1000})]
        (is (= true (async/<!! send)))
        (is (= "bar" (async/<!! recv)))))))

(deftest test-recv-send
  (let [client1 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))
        client2 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))]
    (testing "recv then send"
      (let [recv1 (core/recv! client1 "foo" {:timeout 1000})
            recv2 (core/recv! client2 "foo" {:timeout 1000})
            recv3 (core/recv! client1 "bar" {:timeout 1000 :timeout-val ::timeout})
            send (core/send! client1 "foo" "bar" {:timeout 1000})]
        (is (= "bar" (async/<!! recv1)))
        (is (= "bar" (async/<!! recv2)))
        (is (= ::timeout (async/<!! recv3)))
        (is (= true (async/<!! send)))))))

(deftest test-recv-close
  (let [client1 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))
        client2 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))
        recv-chan (core/recv! client1 "foo" {:timeout 10000})]
    (async/put! (:closer-chan *server*) (constantly true))
    (let [send-chan (core/send! client2 "foo" "bar" {:timeout 5000})]
      (is (= "bar" (async/<!! recv-chan)))
      (is (= true (async/<!! send-chan))))))

(deftest test-send-close
  (let [client1 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))
        client2 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) *port*) {}))
        send-chan (core/send! client1 "foo" "bar" {:timeout 10000})]
    (async/put! (:closer-chan *server*) (constantly true))
    (let [recv-chan (core/recv! client2 "foo" {:timeout 5000})]
      (is (= "bar" (async/<!! recv-chan)))
      (is (= true (async/<!! send-chan))))))