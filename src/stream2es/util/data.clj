(ns stream2es.util.data)

(defn maybe-update-in
  ([m ks f & args]
     (if (get-in m ks)
       (apply update-in m ks f args)
       m)))

