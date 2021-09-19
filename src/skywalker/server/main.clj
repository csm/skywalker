(ns skywalker.server.main
  (:require [clojure.tools.cli :as cli]
            [skywalker.server :as server]
            [skywalker.cluster :as cluster]
            [skywalker.cluster.consul :as consul]
            [skywalker.cluster.health :as health]
            [clojure.core.async :as async]
            [clojure.tools.logging :as log])
  (:import (java.net InetAddress InetSocketAddress)
           (java.security SecureRandom)))

(deftype Msg [args])

(defmethod print-method Msg
  [this w]
  (.write w (apply pr-str (.-args this))))

(add-tap
  (fn [[level & args]]
    (log/log level (->Msg args))))

(def opt-specs
  [["-b" "--bind ADDR" "Set bind address."
    :default (.getHostAddress (InetAddress/getLocalHost))]
   ["-c" "--consul" "Enable consul for discovery."]
   [nil "--consul-url URL" "Set consul API base URL." :default "http://localhost:8500/"]
   [nil "--health-bind ADDR" "Set bind address for health endpoint."
    :default (.getHostAddress (InetAddress/getLocalHost))]
   [nil "--health-port PORT" "Set bind port for health endpoint."
    :parse-fn #(Integer/parseInt %)
    :default 0]
   ["-p" "--port PORT" "Set the port to listen on"
    :parse-fn #(Integer/parseInt %)
    :default 3443]
   ["-r" "--register" "Register self with discovery."]
   ["-s" "--service SVC" "Set the service name." :default "skywalker"]
   ["-h" "--help" "Show help and exit."]])

(def +system+ (atom nil))

(defn -main
  [& args]
  (let [options (cli/parse-opts args opt-specs)]
    (when (:help (:options options))
      (println (:summary options))
      (System/exit 0))
    (when (not-empty (:errors options))
      (doseq [e (:errors options)]
        (println e))
      (System/exit 1))
    (let [health (health/health-server :host (:health-bind (:options options))
                                       :port (:health-port (:options options)))
          discovery (cond
                      (:consul (:options options))
                      (consul/consul-discovery (:service (:options options))
                                               (:consul-url (:options options)))

                      :else nil)
          random (when discovery (SecureRandom.))
          tokens (when discovery (vec (map (fn [_] (.nextLong random)) (range 32))))
          server (server/server (InetSocketAddress. ^String (:bind (:options options))
                                                    ^long (:port (:options options)))
                                :discovery discovery
                                :tokens tokens)]
      (swap! +system+ assoc :health health :server server :discovery discovery)
      (when (and discovery (:register (:options options)))
        (async/<!!
          (cluster/register-node discovery {:bind-address (.getLocalAddress (:socket server))
                                            :health-url (str "http://"
                                                             (:health-bind (:options options))
                                                             \: (:port health)
                                                             "/health")})))
      (println "Skywalker service running.")
      (let [wat (async/alt!! (:server-chan server) :server
                             (:running-chan health) :health)]
        (println wat "exited. I'm stopping now. If that was a mistake, check what happened.")
        (System/exit 0)))))
