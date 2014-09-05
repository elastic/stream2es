(ns stream2es.util.string)

(defn hash-dir
  ([]
     (hash-dir 2))
  ([n]
      (->> (str (System/nanoTime))
           reverse
           (drop 3)
           (take n)
           (interpose java.io.File/separator)
           (apply str))))

(defn rand-str [n coll]
  (->> (for [i (range n)]
         (rand-nth coll))
       (interpose " ")
       (apply str)))
