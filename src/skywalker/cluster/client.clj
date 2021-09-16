(ns skywalker.cluster.client
  (:require [clojure.core.async :as async]
            [clojure.spec.alpha :as s]
            [cognitect.anomalies :as anomalies]
            [msgpack.core :as msgpack]
            [skywalker.core :as core]
            [skywalker.client :as client]
            [skywalker.cluster :as cluster]
            [skywalker.taplog :as log])
  (:import (java.util Collections)
           (com.google.common.hash Hashing Hasher)
           (java.net InetSocketAddress)
           (java.io Closeable)))

(deftype ClusterEntry [token node]
  Comparable
  (compareTo [_ that]
    (compare token (.-token that)))

  Closeable
  (close [_] (.close node)))

(defmethod print-method ClusterEntry
  [e w]
  (.write w (str "(skywalker.cluster.client/->ClusterEntry " (pr-str (.-token e))
                 " " (pr-str (.-node e)) \))))

(defn token-for
  [id]
  (let [h (.newHasher (Hashing/murmur3_128))]
    (.putBytes h ^bytes (msgpack/pack id))
    (.asLong (.hash ^Hasher h))))

(defn- get-node
  [n id]
  (when-not (empty? n)
    (let [i (Collections/binarySearch n (->ClusterEntry (token-for id) nil))
          _ (log/log :trace {:task ::get-node :phase :binary-search :i i})
          i (if (neg? i)
              (let [i (- (inc i))]
                (if (zero? i)
                  (dec (count n))
                  i))
              i)
          _ (log/log :trace {:task ::get-node :phase :made-index :i i :count (count n)})
          node ^ClusterEntry (nth n i)]
      (log/log :trace {:task ::get-node :phase :end :node node})
      (.-node node))))

(defmacro cluster-exec
  [node-mapping change-mult opts id op]
  `(let [node-mapping# ~node-mapping
         op# ~op
         change-mult# ~change-mult
         timeout# (get ~opts :timeout 60000)
         timeout-val# (get ~opts :timeout-val :skywalker.core/timeout)
         changes# (async/chan (async/dropping-buffer 1))
         change-tap# (async/tap change-mult# changes#)
         result-chan# (async/promise-chan)]
     (async/go-loop [timeout# timeout#]
       (if-let [node# (get-node (deref node-mapping#) ~id)]
         (let [_# (log/log :trace {:task ::cluster-client
                                   :phase :got-node
                                   :id ~id
                                   :node node#})
               start# (System/currentTimeMillis)
               res# (async/alt! (op# node#) ([v#] v#)
                                changes# :skywalker.cluster.client/changed)
               end# (System/currentTimeMillis)
               next-timeout# (- timeout# (- end# start#))]
           (log/log :trace {:task ::cluster-exec
                            :phase :got-result
                            :result res#})
           (if (= res# :skywalker.cluster.client/changed)
             (if (pos? next-timeout#)
               (recur next-timeout#)
               (do
                 (async/put! result-chan# timeout-val#)
                 (async/untap change-mult# change-tap#)))
             (do
               (if (nil? res#)
                 (async/close! result-chan#)
                 (async/put! result-chan# res#))
               (async/untap change-mult# change-tap#))))
         (if (pos? (dec timeout#))
           (do
             (log/log :trace {:task ::cluster-exec
                              :phase :waiting-for-nodes})
             (async/alt! change-tap# ::changes
                         (async/timeout 500) :timeout)
             (recur (dec timeout#)))
           timeout-val#)))
     result-chan#))

(deftype ClusterClient [node-mapping change-mult close-chan]
  core/Junction
  (send! [this id value opts]
    (cluster-exec node-mapping change-mult opts id
                  #(core/send! % id value opts)))

  (recv! [this id opts]
    (cluster-exec node-mapping change-mult opts id
                  #(core/recv! % id opts)))

  Closeable
  (close [_]
    (async/close! close-chan)
    (doseq [node @node-mapping] (.close node))))

(defn- ->remote-client
  [{:keys [node/id node/node node/address service/port]} opts]
  (async/go
    (let [junct (async/<! (client/remote-junction (InetSocketAddress. address port)
                                                  (assoc opts :id id :node node)))]
      (if (s/valid? ::anomalies/anomaly junct)
        junct
        (let [tokens (async/<! (client/tokens junct {}))]
          (cond
            (s/valid? ::anomalies/anomaly tokens) tokens

            (= ::client/closed junct)
            {::anomalies/category ::anomalies/interrupted
             ::anomalies/message "socket closed "}

            :else [tokens junct]))))))

(defn peek
  [fn v]
  (fn v)
  v)

(defn cluster-client
  [discovery opts]
  (let [nodes (atom [])
        cluster-change (async/chan)
        cluster-change-mult (async/mult cluster-change)
        close-chan (async/chan)
        discover-loop (async/go-loop []
                        (log/log :debug {:task  ::cluster-client
                                         :phase :begin-discover-loop})
                        (let [results (async/alt! (cluster/discover-nodes discovery) ([v] v)
                                                  close-chan ::closed)]
                          (log/log :debug {:task ::cluster-client
                                           :phase :discovery-done
                                           :results results})
                          (when-not (or (= ::closed results) (nil? results))
                            (when (or (not-empty (:added-nodes results))
                                      (not-empty (:removed-nodes results)))
                              (let [added-nodes (->> (:added-nodes results)
                                                     (map #(->remote-client % opts))
                                                     (async/merge)
                                                     (async/into [])
                                                     (async/<!)
                                                     (peek #(log/log :debug {:task ::cluster-client
                                                                             :phase :created-new-clients
                                                                             :results %}))
                                                     (remove #(s/valid? ::anomalies/anomaly %)))
                                    removed-ids (set (map :id (:removed-nodes results)))
                                    removed? (fn [entry]
                                               (boolean (removed-ids (.-junction_id (.-node entry)))))]
                                (swap! nodes
                                       (fn [n]
                                         (-> (remove removed? n)
                                             (vec)
                                             (into (mapcat (fn [[tokens junct]]
                                                             (map #(->ClusterEntry % junct) tokens))
                                                           added-nodes))
                                             (sort))))
                                (log/log :debug {:task ::cluster-client
                                                 :phase :configured-nodes
                                                 :nodes @nodes})
                                (async/offer! cluster-change true)))
                            (recur))))]
    (->ClusterClient nodes cluster-change-mult close-chan)))
