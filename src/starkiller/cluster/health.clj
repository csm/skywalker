(ns starkiller.cluster.health
  (:require [clojure.core.async :as async])
  (:import (org.eclipse.jetty.server Server ServerConnector Connector)
           (org.eclipse.jetty.servlet ServletHandler ServletHolder)
           (jakarta.servlet.http HttpServlet HttpServletResponse)))

(defn health-server
  "Create a minimal web server bound to bind-address,
  and which will call each function in checks to see
  if it returns a truthy value. If all checks return
  a truthy value, the health check endpoint will return
  a successful, healthy value."
  [& {:keys [host port path checks] :or {host "localhost"
                                         port 0
                                         path "/health"
                                         checks []}}]
  (let [server (Server.)
        connector (ServerConnector. server)
        servlet-handler (ServletHandler.)
        servlet (proxy [HttpServlet] []
                  (doGet [_ res]
                    (let [result (->> checks
                                      (map (fn [f] (boolean (f))))
                                      (reduce #(and %1 %2) true))]
                      (if result
                        (do
                          (.setContentType res "application/json")
                          (.setStatus res HttpServletResponse/SC_OK)
                          (.println (.getWriter res) "{\"status\":\"OK\"}"))
                        (do
                          (.setContentType res "application/json")
                          (.setStatus res HttpServletResponse/SC_SERVICE_UNAVAILABLE)
                          (.println (.getWriter res) "{\"status\":\"ERROR\"}"))))))
        servlet-holder (ServletHolder. servlet)]
    (.setHost connector host)
    (.setPort connector port)
    (.setConnectors server (into-array Connector [connector]))
    (.addServletWithMapping servlet-handler servlet-holder ^String path)
    (.setHandler server servlet-handler)
    (let [running-chan (async/thread (.start server)
                                     (.join server))
          port (.getLocalPort connector)]
      {:server server
       :running-chan running-chan
       :port port})))