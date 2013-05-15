(ns stream2es.log)

(def logger (agent nil))

(defn log [& msg]
  (send logger
        (fn [_]
          (->> msg
               (interpose " ")
               (apply str)
               println))))

(def ^:dynamic *debug* false)

(defmacro debug [& msg]
  (when *debug*
    `(apply log ~(vec msg))))

(defmacro info [& msg]
  `(apply log ~(vec msg)))
