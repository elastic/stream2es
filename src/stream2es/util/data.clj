(ns stream2es.util.data)

(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defn maybe-update-in
  ([m ks f & args]
     (if (get-in m ks)
       (apply update-in m ks f args)
       m)))

(defn remove-in-if
  ([m ks f & args]
     (let [val (get-in m ks)]
       (if (apply f val args)
         (dissoc-in m ks)
         m))))
