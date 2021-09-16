(ns skywalker.async-locking-test
  (:require [clojure.test :refer :all]
            [skywalker.core.impl :refer [async-lock async-locking]]
            [clojure.core.async :as async]))

(deftest test-async-locking
  (testing "that concurrent async processed would clobber each other"
    (let [value (volatile! 0)
          chans (map (fn [i]
                       (async/go
                         (let [v @value]
                           (async/<! (async/timeout (int (rand 50))))
                           (vreset! value (inc v)))))
                     (range 100))]
      (doall chans)
      (is (not= :timeout (async/alt!! (async/into []  (async/merge chans)) :ok
                                      (async/timeout 10000) :timeout)))
      (is (not= 100 @value))))

  (testing "that async locks work"
    (let [value (volatile! 0)
          lock (async-lock)
          chans (map (fn [i]
                       (async/go
                         (async-locking lock
                           (println "running channel" i)
                           (let [v @value]
                             (async/<! (async/timeout (int (rand 50))))
                             (vreset! value (inc v))))))
                     (range 100))]
      (doall chans)
      (is (not= :timeout (async/alt!! (async/into []  (async/merge chans)) :ok
                                      (async/timeout 10000) :timeout)))
      (is (= 100 @value))))

  (testing "that async locks are reentrant"
    (let [lock (async-lock)
          value (volatile! 0)
          mkrunner (fn mkrunner [i]
                     (async/go
                       (async-locking lock
                         (vswap! value inc)
                         (when (pos? i)
                           (async/<! (mkrunner (dec i)))))))
          chans (doall (map mkrunner (range 5)))]
      (is (not= :timeout (async/alt!! (async/into []  (async/merge chans)) :ok
                                      (async/timeout 10000) :timeout)))
      (is (= 15 @value)))))