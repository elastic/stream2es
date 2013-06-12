(ns stream2es.util.time
  (:import (java.text SimpleDateFormat)))

(defn now []
  (.format (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ssZ")
           (java.util.Date.)))

(defn minsecs [secs]
  (let [mins (Math/floor (float (/ secs 60)))
        secs (Math/abs (- secs (* mins 60)))]
    (format "%02d:%06.3f" (int  mins) secs)))
