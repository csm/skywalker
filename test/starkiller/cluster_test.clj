(ns starkiller.cluster-test
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as spec]
            [clojure.test :refer :all]
            [cognitect.anomalies :as anomalies]
            [starkiller.client :as remote]
            [starkiller.cluster :as cluster]
            [starkiller.cluster.client :as client]
            [starkiller.core :as s]
            [starkiller.server :as server]
            [starkiller.taplog :as log])
  (:import (java.net InetSocketAddress)
           (java.security SecureRandom)
           (java.io Closeable)))

(defn mock-discovery
  [chan id]
  (reify cluster/Discovery
    (discover-nodes [_]
      (async/go
        (log/log :debug {:task ::discover-nodes :phase :begin :id id})
        (let [result (async/<! chan)]
          (log/log :debug {:task ::discover-nodes :phase :end :id id :result result})
          result)))
    (register-node [_ _] (async/go))

    Closeable
    (close [_])))

(defn mapped-discovery
  [discovery mapper]
  (reify cluster/Discovery
    (discover-nodes [_]
      (async/go
        (let [result (async/<! (cluster/discover-nodes discovery))]
          (mapper result))))
    (register-node [_ _] (async/go))))

(def ^:dynamic *nodes*)
(def ^:dynamic *client*)
(def ^:dynamic *discovery-chans*)
(def ^:dynamic *removed-nodes*)

(use-fixtures :each
  (fn [f]
    (let [random (SecureRandom.)
          discovery-chans (mapv (fn [i] {:chan (async/chan 16) :id (str "server-" i)}) (range 3))
          discoveries (mapv #(mock-discovery (:chan %) (:id %)) discovery-chans)
          client-chan {:chan (async/chan 16) :id ::test-client}
          client-discovery (mock-discovery (:chan client-chan) ::test-client)
          discovery-chans (conj discovery-chans client-chan)
          server-discovery1 (mapped-discovery (nth discoveries 0)
                                              (fn [{:keys [nodes added-nodes removed-nodes] :as res}]
                                                (when res
                                                  {:nodes nodes
                                                   :added-nodes (set (filter #(not= "server1" (:node/node %)) added-nodes))
                                                   :removed-nodes (set (filter #(not= "server1" (:node/node %)) removed-nodes))})))
          server-discovery2 (mapped-discovery (nth discoveries 1)
                                              (fn [{:keys [nodes added-nodes removed-nodes] :as res}]
                                                (when res
                                                  {:nodes nodes
                                                   :added-nodes (set (filter #(not= "server2" (:node/node %)) added-nodes))
                                                   :removed-nodes (set (filter #(not= "server2" (:node/node %)) removed-nodes))})))
          server-discovery3 (mapped-discovery (nth discoveries 2)
                                              (fn [{:keys [nodes added-nodes removed-nodes] :as res}]
                                                (when res
                                                  {:nodes nodes
                                                   :added-nodes (set (filter #(not= "server3" (:node/node %)) added-nodes))
                                                   :removed-nodes (set (filter #(not= "server3" (:node/node %)) removed-nodes))})))
          tokens1 (mapv (fn [_] (.nextLong random)) (range 32))
          tokens2 (mapv (fn [_] (.nextLong random)) (range 32))
          tokens3 (mapv (fn [_] (.nextLong random)) (range 32))
          s1 (server/server (InetSocketAddress. "127.0.0.1" 0)
                            :tokens tokens1
                            :junction (client/cluster-client server-discovery1
                                                             {:tokens   tokens1
                                                              :junction (s/local-junction)}))
          s2 (server/server (InetSocketAddress. "127.0.0.1" 0)
                            :tokens tokens2
                            :junction (client/cluster-client server-discovery2
                                                             {:tokens tokens2
                                                              :junction (s/local-junction)}))
          s3 (server/server (InetSocketAddress. "127.0.0.1" 0)
                            :tokens tokens3
                            :junction (client/cluster-client server-discovery3
                                                             {:tokens tokens3
                                                              :junction (s/local-junction)}))
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
          client (client/cluster-client client-discovery {:client-id ::test-client})]
      (log/log :debug {:discovery-chans discovery-chans})
      (try
        (binding [*nodes* (atom nodes)
                  *client* client
                  *discovery-chans* discovery-chans
                  *removed-nodes* (atom #{})]
          (doseq [discovery-chan discovery-chans]
            (async/put! (:chan discovery-chan) {:added-nodes (set nodes)}
                        (fn [r]
                          (log/log :info {:task ::use-fixtures
                                          :phase :sent-initial-discovery
                                          :chan discovery-chan
                                          :result r}))))
          (f))
        (finally
          (.close (:socket s1))
          (.close (:socket s2))
          (.close (:socket s3))
          (.close client)
          (doseq [discovery-chan discovery-chans]
            (async/close! (:chan discovery-chan))))))))

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
        (doseq [discovery-chan *discovery-chans*]
          (async/put! (:chan discovery-chan) {:nodes @*nodes*
                                              :removed-nodes #{removed}
                                              :added-nodes #{to-add}})))
      (let [removed (rand-nth (seq @*nodes*))]
        (swap! *removed-nodes* conj removed)
        (swap! *nodes* disj removed)
        (doseq [discovery-chan *discovery-chans*]
          (async/put! (:chan discovery-chan) {:nodes @*nodes*
                                              :removed-nodes #{removed}
                                              :added-nodes #{}}))))
    (when (not-empty @*removed-nodes*)
      (let [to-add (rand-nth (seq @*removed-nodes*))]
        (swap! *removed-nodes* disj to-add)
        (swap! *nodes* conj to-add)
        (doseq [discovery-chan *discovery-chans*]
          (async/put! (:chan discovery-chan) {:nades @*nodes*
                                              :removed-nodes #{}
                                              :added-nodes #{to-add}}))))))

(deftest test-send-recv-chaos
  (testing "that send then recv works during cluster changes"
    (let [send-successes (atom 0)
          recv-successes (atom 0)
          running? (atom true)]
      (loop [tries 100]
        (when (pos? tries)
          (let [send (async/go
                       (async/<! (async/timeout (rand-int 500)))
                       (when (true? (async/<! (s/send! *client* "foo" "bar" {:timeout 1000})))
                         (swap! send-successes inc)))
                _ (mutate!)
                recv (async/go
                       (async/<! (async/timeout (rand-int 500)))
                       (when (= "bar" (async/<! (s/recv! *client* "foo" {:timeout 1000})))
                         (swap! recv-successes inc)))]
            (async/<!! (async/into [] (async/merge [send recv]))))
          (recur (dec tries))))
      (is (= 100 @send-successes))
      (is (= 100 @recv-successes)))))

(deftest test-server-side-clustering
  (testing "that messages are routed server-side"
    (let [clients (->> (deref *nodes*)
                       (map
                         #(remote/remote-junction
                            (InetSocketAddress. "127.0.0.1" ^int (:service/port %))
                            {}))
                       (async/merge)
                       (async/into [])
                       (async/<!!))
          _ (is (not (some #(spec/valid? ::anomalies/anomaly %) clients)))
          recv1 (s/recv! (nth clients 0) :test {:timeout 1000})
          recv2 (s/recv! (nth clients 1) :test {:timeout 1000})
          send (s/send! (nth clients 2) :test :value! {:timeout 1000})]
      (is (= :value! (async/<!! recv1)))
      (is (= :value! (async/<!! recv2)))
      (is (true? (async/<!! send)))
      (doseq [client clients]
        (.close client)))))
