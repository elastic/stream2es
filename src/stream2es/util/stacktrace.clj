(ns stream2es.util.stacktrace)

(defn nice-element [elem]
  (let [[_ package file line]
        (re-find #"([^(]+)\(([^:]+):([0-9]+)\)" (str elem))]
    (format "%7s %-20s %s" line file package)))

(defn stacktrace [throwable]
  (with-out-str
    (let [st (.getStackTrace throwable)]
      (doseq [elem st]
        (println "  " (nice-element elem))))))
