(ns stream2es.test.worker
  (:require [clojure.test :refer :all]
            [stream2es.worker :refer [make-queue*]])
  (:import (java.util.concurrent CountDownLatch)))

(defn make-make-queue [latch results & {:as opts}]
  (let [defaults {:workers 1
                  :process (fn [state obj]
                             (swap! results update-in
                                    [:items] (fnil conj #{}) obj))
                  :notify (fn [uptime workers stats]
                            (.countDown latch))
                  :timeout 5}]
    (make-queue* (merge defaults opts))))

(deftest basic-queue
  (let [results (atom {})
        latch (CountDownLatch. 1)
        publish (make-make-queue latch results)]
    (dotimes [n 10]
      (publish n))
    (publish :eof)
    (.await latch)
    (is (= @results {:items (into #{} (range 10))}))))

(deftest drop-msgs
  (let [results (atom {})
        latch (CountDownLatch. 1)
        publish (make-make-queue
                 latch results
                 :opts {:drop 1}
                 :enqueue? (fn [obj curr opts]
                             (> curr (:drop opts))))]
    (dotimes [n 3]
      (publish n))
    (publish :eof)
    (.await latch)
    (is (= @results {:items #{1 2}}))))
