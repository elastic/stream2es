(ns stream2es.log)

(def ^:dynamic *debug* false)

(defmacro debug [& msg]
  (when *debug*
    `(apply println ~(vec msg))))

(defmacro info [& msg]
  `(apply println ~(vec msg)))

