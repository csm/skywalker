(ns skywalker.cluster.consul
  (:require [clojure.core.async :as async]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [cognitect.anomalies :as anomalies]
            [skywalker.cluster :as cluster]
            [skywalker.taplog :as log])
  (:import (java.net URI InetAddress)
           (java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers HttpRequest$BodyPublishers HttpResponse)
           (java.util.function BiConsumer)
           (java.nio.charset StandardCharsets)))

(defn consul-discovery
  "Create and return a Discovery instance that reads from consul.

  service is a string giving the service name; this will query
  consul for

  uri is the consul URL of the consul instance to contact."
  [service uri & {:keys [wait ignore-self] :or {wait "60s" ignore-self true}}]
  (let [nodes (atom #{})
        index (atom nil)
        client (HttpClient/newHttpClient)
        uri (if (instance? URI uri) uri (URI. uri))]
    (reify cluster/Discovery
      (discover-nodes [_]
        (log/log :debug {:task ::discover-nodes
                         :phase :begin
                         :uri uri})
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
                (log/log :debug {:task ::discover-nodes
                                 :phase :end
                                 :result result
                                 :exception exception})
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
                        hostname (.getHostName (InetAddress/getLocalHost))
                        healthy-nodes (->> (json/read (io/reader (.body response)) :key-fn keyword)
                                           (map
                                             (fn [node]
                                               {:node/id (-> node :Node :ID)
                                                :node/node (-> node :Node :Node)
                                                :node/address (-> node :Node :Address)
                                                :service/address (-> node :Service :Address)
                                                :service/port (-> node :Service :Port)}))
                                           (filter (fn [node]
                                                     (or (not ignore-self)
                                                         (not= (:node/node node) hostname))))
                                           (set))
                        added-nodes (set/difference healthy-nodes current-nodes)
                        removed-nodes (set/difference current-nodes healthy-nodes)]
                    (reset! nodes healthy-nodes)
                    (reset! index (-> response (.headers) (.firstValue "x-consul-index") (.orElse nil)))
                    (log/log :debug {:nodes healthy-nodes :index index})
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
                (log/log :debug {:task ::register-node :phase :end :result res :exception ex})
                (if (some? ex)
                  (async/put! channel {::anomalies/category ::anomalies/fault
                                       ::anomalies/message (.getMessage ^Throwable ex)
                                       :cause ex})
                  (async/put! channel true)))))
          channel)))))
