(ns skywalker.remote-test
  (:require [clojure.test :refer :all]
            [skywalker.client :as client]
            [skywalker.server :as server]
            [clojure.core.async :as async]
            [skywalker.core :as core])
  (:import (java.net InetSocketAddress InetAddress)))

(comment
  (add-tap (comp println pr-str)))

(def ^:dynamic *port*)

(use-fixtures :each
  (fn [f]
    (let [server (server/server (InetSocketAddress. (InetAddress/getLocalHost) 0))
          port (.getPort (.getLocalAddress (:socket server)))]
      (binding [*port* port]
        (f))
      (.close (:socket server)))))

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

