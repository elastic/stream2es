(ns wiki2es.help
  (:require [clojure.tools.cli :refer [cli]]))

(defn help [specs]
  (doseq [spec (sort-by second specs)]
    (let [[_ switch desc & {:keys [default]}] spec]
      (println (format "%-15s %s (default: %s)" switch desc default)))))
