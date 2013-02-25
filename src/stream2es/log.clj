(ns stream2es.log)

(def logger
  (agent nil))

(defn infof
  ([s]
     (infof "%s" s))
  ([fmt & s]
     (send logger
           (fn [_]
             (println (apply format fmt s))
             (flush)))))

(defn info
  ([& s]
     (->> s
          (interpose " ")
          (apply str)
          (infof "%s"))))

