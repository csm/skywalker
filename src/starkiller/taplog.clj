(ns starkiller.taplog
  (:require [clojure.tools.logging :as log]))

(defonce installed
         (add-tap
           (fn [{:keys [logger-ns level throwable message]}]
             (log/log logger-ns level throwable message))))

(defmacro log
  ([level message]
   `(log ~level nil ~message))
  ([level throwable message]
   `(log ~*ns* ~level ~throwable ~message))
  ([logger-ns level throwable message]
   `(tap> {:logger-ns ~logger-ns
           :level ~level
           :throwable ~throwable
           :message ~message})))