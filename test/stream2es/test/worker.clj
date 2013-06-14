(ns stream2es.test.worker
  (:require [clojure.test :refer :all]
            [stream2es.main :as main]
            [stream2es.worker :refer [make-queue*]])
  (:import (java.util.concurrent CountDownLatch
                                 TimeUnit)))

(defn make-make-queue [latch results & {:as opts}]
  (let [defaults {:process (fn [state obj]
                             (.sleep TimeUnit/MILLISECONDS (rand-int 100))
                             (swap! results update-in
                                    [:items] (fnil conj #{}) obj))
                  :notify (fn [uptime workers stats]
                            (.countDown latch))
                  :timeout 5
                  :workers 3}]
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
    (publish :drop-0)
    (publish :drop-1)
    (publish :drop-2)
    (publish :eof)
    (.await latch)
    (is (= @results {:items #{:drop-1 :drop-2}}))))

(deftest terminate-stream
  (let [results (atom {})
        latch (CountDownLatch. 1)
        publish (make-make-queue
                 latch results
                 :opts {:take 1
                        :drop 0}
                 :stop-streaming? main/stop-streaming?)]
    (publish :term-0)
    (publish :term-1)
    (.await latch 2 TimeUnit/SECONDS)
    (is (= @results {:items #{:term-0}}))
    (is (= :dead (publish :another)))))
