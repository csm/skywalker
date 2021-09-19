(ns starkiller.cluster.dns
  (:require [starkiller.cluster :as cluster]
            [clojure.core.async :as async])
  (:import (java.util.concurrent TimeUnit)
           (java.net InetAddress)))

(defn dns-discovery
  [hostname & {:keys [wait wait-unit] :or {wait 1000 wait-unit TimeUnit/MILLISECONDS}}]
  (let [first (atom true)]
    (reify cluster/Discovery
      (discover-nodes [_]
        (async/go
          (when-not (compare-and-set! first true false)
            (async/<! (async/timeout (.convert TimeUnit/MILLISECONDS wait wait-unit))))
          (let [results (async/<! (async/thread (InetAddress/getAllByName hostname)))]))))))