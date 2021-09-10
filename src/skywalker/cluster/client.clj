(ns skywalker.cluster.client
  (:require [cognitect.anomalies :as anomalies]
            [skywalker.core :as core]
            [msgpack.core :as msgpack]
            [clojure.core.async :as async]
            [skywalker.client :as client]
            [skywalker.cluster :as cluster]
            [clojure.spec.alpha :as s])
  (:import (java.util Collections)
           (com.google.common.hash Hashing Hasher)
           (java.net InetSocketAddress)))

(deftype ClusterEntry [token node]
  Comparable
  (compareTo [_ that]
    (compare token (.-token that))))

(defn token-for
  [id]
  (let [h (.newHasher (Hashing/murmur3_128))]
    (.putBytes h ^bytes (msgpack/pack id))
    (.asLong (.hash ^Hasher h))))

(defn- get-node
  [n id]
  (let [i (Collections/binarySearch n (->ClusterEntry (token-for id) nil))
        i (if (neg? i)
            (let [i (- (inc i))]
              (if (zero? i)
                (dec (count n))
                i))
            i)
        node ^ClusterEntry (nth n i)]
    (.-node node)))

(defmacro cluster-exec
  [node-mapping change-mult opts id op]
  `(let [op# ~op
         timeout# (get ~opts :timeout 60000)
         timeout-val# (get ~opts :timeout-val :skywalker.core/timeout)
         changes# (async/chan (async/dropping-buffer 1))
         change-tap# (async/tap change-mult changes#)
         result-chan# (async/promise-chan)]
     (async/go-loop [timeout# timeout#]
       (let [node# (get-node (deref ~node-mapping) ~id)
             start# (System/currentTimeMillis)
             res# (async/alt! (op# node#) ([v#] v#)
                              changes# :skywalker.cluster.client/changed)
             end# (System/currentTimeMillis)
             next-timeout# (- timeout# (- end# start#))]
         (if (= res# :skywalker.cluster.client/changed)
           (if (pos? next-timeout#)
             (recur next-timeout#)
             (do
               (async/put! result-chan# timeout-val#)
               (async/untap ~change-mult change-tap#)))
           (do
             (if (nil? res#)
               (async/close! result-chan#)
               (async/put! result-chan# res#))
             (async/untap ~change-mult change-tap#)))))))

(deftype ClusterClient [node-mapping change-mult]
  core/Junction
  (send! [this id value opts]
    (cluster-exec node-mapping change-mult opts id
                  #(core/send! % id value opts)))

  (recv! [this id opts]
    (cluster-exec node-mapping change-mult opts id
                  #(core/recv! % id opts))))

(defn- ->remote-client
  [{:keys [node/id node/node service/address service/port]} opts]
  (async/go
    (let [junct (async/<! (client/remote-junction (InetSocketAddress. address port)
                                                  (assoc opts :id id :node node)))]
      (if (s/valid? ::anomalies/anomaly junct)
        junct
        (let [tokens (async/<! (client/tokens junct {}))]
          (if (s/valid? ::anomalies/anomaly tokens)
            tokens
            [tokens junct]))))))

(defn cluster-client
  [discovery opts]
  (let [nodes (atom [])
        custer-change (async/chan)
        cluster-change-mult (async/mult (async/chan))
        discover-loop (async/go-loop []
                        (let [results (async/<! (cluster/discover-nodes discovery))]
                          (when (or (not-empty (:added-nodes results))
                                    (not-empty (:removed-nodes results)))
                            (let [added-nodes (->> (:added-nodes results)
                                                   (map ->remote-client)
                                                   (async/merge)
                                                   (async/into [])
                                                   (remove #(s/valid? ::anomalies/anomaly %)))
                                  removed-ids (set (map :id (:removed-nodes)))
                                  removed? (fn [[_ junct]]
                                             (boolean (removed-ids (.-id junct))))]
                              (swap! nodes
                                     (fn [n]
                                       (-> (remove removed? n)
                                           (vec)
                                           (into (mapcat (fn [[tokens junct]]
                                                           (map #(->ClusterEntry % junct) tokens))
                                                         added-nodes))
                                           (sort))))
                              (async/put!)))
                          (recur)))]))
