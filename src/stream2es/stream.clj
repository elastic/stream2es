(ns stream2es.stream)

(defn type-dispatch [& args]
  (type (first args)))

(defn sym-dispatch [& args]
  (first args))

(defn no-type [& args]
  (throw (Exception. (format "what's a %s" (apply type-dispatch args)))))

(defmulti make-source type-dispatch :default no-type)

(defmulti make-stream sym-dispatch)

(defmulti cmd-specs sym-dispatch)
