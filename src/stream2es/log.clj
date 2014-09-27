(ns stream2es.log
  (:refer-clojure :exclude [flush])
  (:import (java.util.concurrent Executors)))

(def svc (Executors/newFixedThreadPool 1))

(def logger-agent (agent nil))

(defn print-log [msg]
  (->> msg
       (interpose " ")
       (apply str)
       println))

(defn log [& msg]
  (send-via svc logger-agent
            (fn [_]
              (print-log msg))))

(def ^:dynamic *debug* false)

(def ^:dynamic *trace* false)

(defmacro trace [& msg]
  (when *trace*
    `(apply log ~(vec msg))))

(defmacro debug [& msg]
  (when *debug*
    `(apply log ~(vec msg))))

(defmacro info [& msg]
  `(apply log ~(vec msg)))

(defn flush []
  (dotimes [_ 3]
    (clojure.core/flush)
    (Thread/sleep (rand-int 100))))
