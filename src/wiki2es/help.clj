(ns wiki2es.help
  (:require [clojure.tools.cli :as cli]))

(defn line-up-switches [sws]
  (->> (if (= 1 (count sws))
         (cons "  " sws)
         sws)
       (filter #(not (.startsWith % "--no-")))
       (interpose " ")
       (apply str)))

(defn maybe-string [thing]
  (if (string? thing)
    (format "\"%s\"" thing)
    thing))

(defn help [specs]
  (with-out-str
    (doseq [spec (->> specs
                      (map #'cli/generate-spec)
                      (sort-by :name))]
      (let [{:keys [default switches docs]} spec]
        (println (format "%-15s %s (default: %s)"
                         (line-up-switches switches)
                         docs
                         (maybe-string default)))))))
