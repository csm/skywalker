(ns skywalker.cluster.consul
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [cognitect.anomalies :as anomalies]
            [skywalker.cluster :as cluster])
  (:import (java.net URI InetAddress)
           (java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers HttpRequest$BodyPublishers HttpResponse)
           (java.util.function BiConsumer)
           (java.nio.charset StandardCharsets)))

(defn consul-discovery
  "Create and return a Discovery instance that reads from consul.

  service is a string giving the service name; this will query
  consul for

  uri is the consul URL of the consul instance to contact."
  [service uri & {:keys [wait] :or {wait "60s"}}]
  (let [nodes (atom #{})
        index (atom nil)
        client (HttpClient/newHttpClient)
        uri (if (instance? URI uri) uri (URI. uri))]
    (reify cluster/Discovery
      (discover-nodes [_]
        (let [uri (.resolve uri (str "/v1/health/service/" service "?wait=" wait "&passing=true"
                                     (when @index (str "&index=" @index))))
              request (-> (HttpRequest/newBuilder)
                          (.GET)
                          (.uri uri)
                          (.build))
              future (.sendAsync client request
                                 (HttpResponse$BodyHandlers/ofByteArray))
              channel (async/promise-chan)]
          (.whenComplete future
            (reify BiConsumer
              (accept [_ result exception]
                (if (some? exception)
                  (async/put! channel {::anomalies/category ::anomalies/fault
                                       ::anomalies/message (.getMessage ^Throwable exception)
                                       :cause exception})
                  (async/put! channel result)))))
          (async/go
            (let [response (async/<! channel)]
              (if (s/valid? ::anomalies/anomaly response)
                {:nodes @nodes
                 :error response}
                (if (= (.statusCode response) 200)
                  (let [current-nodes @nodes
                        healthy-nodes (set
                                        (map
                                          (fn [node]
                                            {:node/id (-> node :Node :ID)
                                             :node/node (-> node :Node :Node)
                                             :service/address (-> node :Service :Address)
                                             :service/port (-> node :Service :Port)})
                                          (json/read (io/reader (.body response)) :key-fn keyword)))
                        added-nodes (set/difference healthy-nodes current-nodes)
                        removed-nodes (set/difference current-nodes healthy-nodes)]
                    (reset! nodes healthy-nodes)
                    (reset! index (-> response (.headers) (.firstValue "x-consul-index") (.orElse nil)))
                    (tap> {:nodes healthy-nodes :index index})
                    {:nodes healthy-nodes
                     :added-nodes added-nodes
                     :removed-nodes removed-nodes})
                  {:nodes @nodes
                   :error {::anomalies/category ::anomalies/fault
                           ::anomalies/message  (str "consul request failed with " (.statusCode response))
                           :body                (String. ^bytes (.body response) StandardCharsets/UTF_8)}}))))))

      (register-node [this opts]
        (let [{:keys [bind-address health-url id] :or {id (.getHostName (InetAddress/getLocalHost))}} opts
              uri (.resolve uri (str "/v1/agent/service/register/"))
              request (-> (HttpRequest/newBuilder)
                          (.PUT (HttpRequest$BodyPublishers/ofString
                                  (json/write-str
                                    {:Name service
                                     :Address (.getHostAddress (.getAddress bind-address))
                                     :Port (.getPort bind-address)
                                     :Check {:Interval "30s"
                                             :Timeout "5s"
                                             :HTTP health-url}})))
                          (.header "Content-type" "application/json")
                          (.uri uri)
                          (.build))
              future (.sendAsync client request
                                 (HttpResponse$BodyHandlers/ofByteArray))
              channel (async/promise-chan)]
          (.whenComplete future
            (reify BiConsumer
              (accept [_ res ex]
                (tap> [:debug "register service result" res "ex" ex])
                (if (some? ex)
                  (async/put! channel {::anomalies/category ::anomalies/fault
                                       ::anomalies/message (.getMessage ^Throwable ex)
                                       :cause ex})
                  (async/put! channel true)))))
          channel)))))
