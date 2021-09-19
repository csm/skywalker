(ns starkiller.core-test
  (:require [clojure.test :refer :all]
            [starkiller.core :as core]
            [clojure.core.async :as async]))

(deftest test-recv-timeout
  (let [junct (core/local-junction)]
    (is (= :timeout (async/<!! (core/recv! junct "foo" {:timeout 1000 :timeout-val :timeout}))))))

(deftest test-send-timeout
  (let [junct (core/local-junction)]
    (is (= :timeout (async/<!! (core/send! junct "foo" "bar" {:timeout 1000 :timeout-val :timeout}))))))

(deftest test-send-recv
  (let [junct (core/local-junction)
        sender (core/send! junct "foo" "bar" {:timeout 1000})
        receiver (core/recv! junct "foo" {:timeout 1000})]
    (is (= true (async/<!! sender)))
    (is (= "bar" (async/<!! receiver)))))

(deftest test-recv-send
  (let [junct (core/local-junction)
        receiver1 (core/recv! junct "foo" {:timeout 1000})
        receiver2 (core/recv! junct "foo" {:timeout 1000})
        receiver3 (core/recv! junct "bar" {:timeout 1000 :timeout-val :timeout})
        sender (core/send! junct "foo" "bar" {:timeout 1000})]
    (is (= "bar" (async/<!! receiver1)))
    (is (= "bar" (async/<!! receiver2)))
    (is (= :timeout (async/<!! receiver3)))
    (is (= true (async/<!! sender)))))