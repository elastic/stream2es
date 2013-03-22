(ns stream2es.util.string)

(defn hash-dir
  ([]
     (hash-dir 2))
  ([n]
      (->> (str (System/nanoTime))
           reverse
           (take n)
           (interpose java.io.File/separator)
           (apply str))))
