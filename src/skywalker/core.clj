(ns skywalker.core
  (:require [clojure.core.async :as async])
  (:import (com.google.common.cache LoadingCache CacheBuilder CacheLoader)
           (java.time Duration)
           (com.google.common.base Supplier)))

(defprotocol Junction
  (send! [this id value opts]
    "Attempt to send value to id. Opts may include:

    - :timeout The send timeout, in millis (default 60000)
    - :timeout-val The value to return on timeout. Default ::timeout

    Returns a promise channel that will yield the value, a timeout
    value, or an Exception on error.")

  (recv! [this id opts]
    "Attempt to receive a value on id. Opts may include:

    - :timeout The send timeout, in millis (default 60000)
    - :timeout-val The value to return on timeout. Default ::timeout

    Returns a promise channel"))

(deftype LocalJunction [^LoadingCache chans]
  Junction
  (send! [_ id value opts]
    (async/go
      (let [{:keys [timeout timeout-val]
             :or {timeout 60000 timeout-val ::timeout}} opts
            chan (.get chans id)
            start (System/currentTimeMillis)
            recv (async/alt!
                   chan ([v] v)
                   (async/timeout timeout) :timeout)
            end (System/currentTimeMillis)]
        (if (= recv :timeout)
          timeout-val
          (loop []
            (if-let [recv (async/poll! chan)]
              (do
                (async/put! recv value)
                (recur))
              true))))))

  (recv! [_ id opts]
    (async/go
      (let [{:keys [timeout timeout-val]
             :or {timeout 60000 timeout-val ::timeout}} opts
            chan (.get chans id)
            recv (async/promise-chan)]
        (case (async/alt!
                [[chan recv]] :ok
                (async/timeout timeout) :timeout)
          :ok (async/alt!
                recv ([v] v)
                (async/timeout timeout) timeout-val)
          :timeout timeout-val)))))

(defn local-junction
  []
  (->LocalJunction (-> (CacheBuilder/newBuilder)
                       (.expireAfterAccess (Duration/ofMinutes 5))
                       (.build
                         (CacheLoader/from
                           (reify Supplier
                             (get [_] (async/chan))))))))
