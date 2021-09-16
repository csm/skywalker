(ns skywalker.cluster-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer :all]
            [skywalker.core :as s]
            [skywalker.cluster :as cluster]
            [skywalker.cluster.client :as client]
            [skywalker.server :as server])
  (:import (java.net InetSocketAddress)))

(defn mock-discovery
  [chan]
  (reify cluster/Discovery
    (discover-nodes [this] chan)
    (register-node [_ _] (async/go))))

(def ^:dynamic *nodes*)
(def ^:dynamic *client*)
(def ^:dynamic *discovery-chan*)
(def ^:dynamic *removed-nodes*)

(use-fixtures :each
  (fn [f]
    (let [s1 (server/server (InetSocketAddress. "127.0.0.1" 0))
          s2 (server/server (InetSocketAddress. "127.0.0.1" 0))
          s3 (server/server (InetSocketAddress. "127.0.0.1" 0))
          nodes #{{:node/id "s1"
                   :node/node "server1"
                   :node/address "127.0.0.1"
                   :service/address ""
                   :service/port (-> s1 :socket (.getLocalAddress) (.getPort))}
                  {:node/id "s2"
                   :node/node "server2"
                   :node/address "127.0.0.1"
                   :service/address ""
                   :service/port (-> s2 :socket (.getLocalAddress) (.getPort))}
                  {:node/id "s3"
                   :node/node "server3"
                   :node/address "127.0.0.1"
                   :service/address ""
                   :service/port (-> s3 :socket (.getLocalAddress) (.getPort))}}
          discovery-chan (async/chan)
          discovery (mock-discovery discovery-chan)
          client (client/cluster-client discovery {})]
      (try
        (binding [*nodes* (atom nodes)
                  *client* client
                  *discovery-chan* discovery-chan
                  *removed-nodes* (atom #{})]
          (async/put! discovery-chan {:added-nodes (set nodes)})
          (f))
        (finally
          (.close (:socket s1))
          (.close (:socket s2))
          (.close (:socket s3))
          (.close client)
          (async/close! discovery-chan))))))

(deftest test-send-timeout
  (testing "send! timeouts"
    (let [result (async/<!! (s/send! *client* "foo" "bar" {:timeout 1000
                                                           :timeout-val "timeout"}))]
      (is (= result "timeout")))))


(deftest test-recv-timeout
  (testing "recv! timeouts"
    (let [result (async/<!! (s/recv! *client* "foo" {:timeout 1000
                                                     :timeout-val "timeout"}))]
      (is (= result "timeout")))))

(deftest test-send-recv
  (testing "send then recv"
    (let [send-chan (s/send! *client* "foo" "bar" {:timeout 1000
                                                   :timeout-val "timeout"})
          recv-chan (s/recv! *client* "foo" {:timeout 1000
                                             :timeout-val "timeout"})]
      (is (= true (async/<!! send-chan)))
      (is (= "bar" (async/<!! recv-chan))))))

(deftest test-recv-send
  (testing "recv then send"
    (let [recv-chan (s/recv! *client* "foo" {:timeout 1000
                                             :timeout-val "timeout"})
          send-chan (s/send! *client* "foo" "bar" {:timeout 1000
                                                   :timeout-val "timeout"})]
      (is (= "bar" (async/<!! recv-chan)))
      (is (= true (async/<!! send-chan))))))

(defn mutate!
  []
  (if (even? (rand-int 2))
    (if (= 1 (count @*nodes*))
      (let [removed (first @*nodes*)
            to-add (rand-nth (seq @*nodes*))]
        (swap! *removed-nodes* disj to-add)
        (swap! *removed-nodes* conj removed)
        (swap! *nodes* conj to-add)
        (async/put! *discovery-chan* {:nodes @*nodes*
                                      :removed-nodes #{removed}
                                      :added-nodes #{to-add}}))
      (let [removed (rand-nth (seq @*nodes*))]
        (swap! *removed-nodes* conj removed)
        (swap! *nodes* disj removed)
        (async/put! *discovery-chan* {:nodes @*nodes*
                                      :removed-nodes #{removed}
                                      :added-nodes #{}})))
    (when (not-empty @*removed-nodes*)
      (let [to-add (rand-nth (seq @*removed-nodes*))]
        (swap! *removed-nodes* disj to-add)
        (swap! *nodes* conj to-add)
        (async/put! *discovery-chan* {:nades @*nodes*
                                      :removed-nodes #{}
                                      :added-nodes #{to-add}})))))

(deftest test-send-recv-chaos
  (testing "that send then recv works during cluster changes"
    (let [send-successes (atom 0)
          recv-successes (atom 0)
          running? (atom true)]
      (async/go-loop []
        (when @running?
          (async/<! (async/timeout (rand-int 500)))
          (mutate!)
          (recur)))
      (let [sends (async/go-loop [n 100]
                    (when (pos? n)
                      (async/<! (async/timeout (rand-int 500)))
                      (when (true? (async/<! (s/send! *client* "foo" "bar" {})))
                        (swap! send-successes inc))
                      (recur (dec n))))
            recvs (async/go-loop [n 100]
                    (when (pos? n)
                      (async/<! (async/timeout (rand-int 500)))
                      (when (= "bar" (async/<! (s/recv! *client* "foo" {})))
                        (swap! recv-successes inc))
                      (recur (dec n))))]
        (async/alt!! (async/into [] (async/merge [sends recvs])) :done
                     (async/timeout 60000) :timeout)
        (is (= 100 @send-successes))
        (is (= 100 @recv-successes))))))
