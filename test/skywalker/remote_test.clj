(ns skywalker.remote-test
  (:require [clojure.test :refer :all]
            [skywalker.core.client :as client]
            [skywalker.core.server :as server]
            [clojure.core.async :as async]
            [skywalker.core :as core])
  (:import (java.net InetSocketAddress InetAddress)
           (java.nio.channels AsynchronousServerSocketChannel)))

(add-tap println)

(deftest test-client-server
  (let [server (server/server (InetSocketAddress. (InetAddress/getLocalHost) 0))
        port (.getPort (.getLocalAddress (:socket server)))
        client1 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) port) {}))
        client2 (async/<!! (client/remote-junction (InetSocketAddress. (InetAddress/getLocalHost) port) {}))]
    (testing "send timeout"
      (let [result (async/<!! (core/send! client1 "foo" "bar" {:timeout 1000 :timeout-val ::timeout}))]
        (is (= result ::timeout))))
    (testing "recv timeout"
      (let [result (async/<!! (core/recv! client1 "foo" {:timeout 1000 :timeout-val ::timeout}))]
        (is (= result ::timeout))))
    (testing "send then recv"
      (let [send (core/send! client1 "foo" "bar" {:timeout 1000})
            recv (core/recv! client2 "foo" {:timeout 1000})]
        (is (= true (async/<!! send)))
        (is (= "bar" (async/<!! recv)))))
    (testing "recv thes send"
      (let [recv1 (core/recv! client1 "foo" {:timeout 1000})
            recv2 (core/recv! client2 "foo" {:timeout 1000})
            recv3 (core/recv! client1 "bar" {:timeout 1000 :timeout-val ::timeout})
            send (core/send! client1 "foo" "bar" {:timeout 1000})]
        (is (= "foo" (async/<!! recv1)))
        (is (= "foo" (async/<!! recv2)))
        (is (= ::timeout (async/<!! recv3)))
        (is (= true (async/<!! send)))))))

