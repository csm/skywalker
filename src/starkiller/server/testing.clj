(ns starkiller.server.testing
  (:require [starkiller.server :as server]
            [starkiller.cluster :as cluster]
            [clojure.core.async :as async]
            [starkiller.cluster.client :as cc]
            [starkiller.core :as core])
  (:import (java.net InetSocketAddress InetAddress)
           (java.security SecureRandom)))

(defn test-server
  "Create a single test server, bound to the loopback address and any port."
  []
  (server/server (InetSocketAddress. (InetAddress/getLoopbackAddress) 0)))

(defn test-cluster
  ([] (test-cluster 3))
  ([num-servers]
   (let [discovery-chans (mapv (fn [_] (async/chan)) (range num-servers))
         discoveries (mapv #(reify cluster/Discovery
                              (discover-nodes [_] %)
                              (register-node [_ _] (async/go)))
                           discovery-chans)
         random (SecureRandom.)
         tokens (mapv #(vec (for [_ (range 32)] (.nextLong random)))
                      (range num-servers))
         servers (mapv #(server/server (InetSocketAddress. (InetAddress/getLoopbackAddress) 0)
                                       :tokens (nth tokens %)
                                       :junction (cc/cluster-client (nth discoveries %)
                                                                    {:tokens   (nth tokens %)
                                                                     :junction (core/local-junction)}))
                       (range num-servers))
         nodes (mapv #(hash-map {:node/id (str "server" %)
                                 :node/node (str "server" %)
                                 :node/address ""
                                 :service/address (.getHostAddress (InetAddress/getLoopbackAddress))
                                 :service/port (:port (nth servers %))})
                     (range num-servers))]
     (doseq [chan discovery-chans]
       (async/put! chan {:added-nodes nodes}))
     servers)))