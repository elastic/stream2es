(ns stream2es.util.rate
  (:refer-clojure :exclude [bytes])
  (:require [stream2es.util.time :as time]))

(defn div0 [a b]
  (if (pos? b) (float (/ a b)) 0))

(defn n-per-sec [n secs]
  (div0 n secs))

(defn kb [bytes]
  (float (/ bytes 1024)))

(defn mb [bytes]
  (float (/ (kb bytes) 1024)))

(defn gb [bytes]
  (float (/ (mb bytes) 1024)))

(defn kb-per-sec [bytes secs]
  (n-per-sec (kb bytes) secs))

(defn mb-per-sec [bytes secs]
  (n-per-sec (mb bytes) secs))

(defn calc [start-ms end-ms docs bytes]
  (let [total-ms (- end-ms start-ms)
        upsecs (time/secs total-ms)]
    {:start-ms start-ms
     :end-ms end-ms
     :total-ms total-ms
     :total-s upsecs
     :minsecs (time/minsecs upsecs)
     :docs docs
     :docs-per-sec (n-per-sec docs upsecs)
     :bytes bytes
     :kb (kb bytes)
     :mb (mb bytes)
     :gb (gb bytes)
     :kb-per-sec (kb-per-sec bytes upsecs)
     :mb-per-sec (mb-per-sec bytes upsecs)}))
