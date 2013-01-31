(ns wiki2es.main
  (:gen-class)
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.tools.cli :refer [cli]]
            [wiki2es.log :as log]
            [wiki2es.size :refer [size-of]]
            [wiki2es.version :refer [version]]
            [wiki2es.xml :as xml])
  (:import (java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue)))

(def bulk-bytes
  (* 3 1024 1024))

(def latest-wiki
  (str "http://download.wikimedia.org"
       "/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"))

(def opts
  [["-b" "--maxbytes" "Bulk size in bytes"
    :default bulk-bytes
    :parse-fn #(Integer/parseInt %)]
   ["-d" "--maxdocs" "Number of docs to index"
    :default 500
    :parse-fn #(Integer/parseInt %)]
   ["-i" "--index" "ES index"
    :default "wiki"]
   ["-q" "--queue" "Size of the internal bulk queue"
    :default 20
    :parse-fn #(Integer/parseInt %)]
   ["-s" "--skip" "Skip this many docs before indexing"
    :default 0
    :parse-fn #(Integer/parseInt %)]
   ["-t" "--type" "ES type"
    :default "page"]
   ["-u" "--url" "Wiki dump locator"
    :default latest-wiki]
   ["-v" "--version" "Print version"
    :flag true
    :default false]
   ["-w" "--workers" "Number of indexing threads"
    :default 2
    :parse-fn #(Integer/parseInt %)]])

(defrecord BulkItem [meta source])

(defn quit
  ([]
     (quit "done"))
  ([s]
     (quit "%s" s))
  ([fmt & s]
     (shutdown-agents)
     (println (apply format fmt s))
     (System/exit 0)))

(defn page2item [_index _type offset page]
  (let [metadoc {:index
                 {:_index _index
                  :_type _type
                  :_id (.getID page)}}
        source {:dumpoffset offset
                :title (-> page .getTitle str .trim)
                :text (-> (.getText page) .trim)
                :redirect (.isRedirect page)
                :special (.isSpecialPage page)
                :stub (.isStub page)
                :disambiguation (.isDisambiguationPage page)
                :category (.getCategories page)
                :link (.getLinks page)}]
    (BulkItem.
     metadoc
     (merge source {:bytes (size-of source)}))))

(defn add-doc [state page]
  (let [item (page2item (:index @state) (:type @state)
                        (:curr @state) page)]
    (alter state update-in [:bytes] + (-> item :source :bytes))
    (alter state update-in [:items] conj item)))

(defn flush-bulk [state]
  (let [itemct (count (:items @state))
        items (:items @state)]
    (when (pos? itemct)
      (log/infof
       ">--> push bulk: items:%d bytes:%d first-id:%s"
       itemct (:bytes @state)
       (-> items first :meta :index :_id)))
    ((:indexer @state) items)
    (alter state assoc :bytes 0)
    (alter state assoc :items [])))

(defn maybe-index [state]
  (let [{:keys [bytes maxbytes]} @state]
    (when (> bytes maxbytes)
      (flush-bulk state))))

(defn continue? [state]
  (let [{:keys [skip maxdocs curr]} @state]
    (if (pos? maxdocs)
      (< curr (+ skip maxdocs))
      true)))

(defn flush-indexer [state]
  ((:indexer @state) :stop))

(defn want-shutdown [state]
  (flush-bulk state)
  (flush-indexer state))

(defn make-handler [state]
  (fn [page]
    (dosync
     (if (continue? state)
       (do
         (alter state update-in [:curr] inc)
         (when (> (:curr @state) (:skip @state))
           (add-doc state page)
           (maybe-index state)))
       (want-shutdown state)))))

(defn post [data]
  (http/post "http://localhost:9200/_bulk" {:body data}))

(defn make-indexable-bulk [items]
  (->> (for [item items]
         (str (json/encode (:meta item))
              "\n"
              (json/encode (:source item))
              "\n"))
       (apply str)))

(defn index-bulk [q total]
  (let [bulk (.take q)]
    (when (and (sequential? bulk) (pos? (count bulk)))
      (log/info "<--< pull bulk:" (count bulk) "items")
      (post (make-indexable-bulk bulk)))
    (when-not (= :stop bulk)
      (swap! total + (count bulk))
      (recur q total))))

(defn start-indexer-pool [opts]
  (let [q (LinkedBlockingQueue. (:queue opts))
        latch (CountDownLatch. (:workers opts))
        total (atom 0)
        disp (fn []
               (index-bulk q total)
               (.countDown latch))
        kill (fn []
               (.await latch)
               (quit "processed %d docs" @total))]
    ;; start index pool
    (dotimes [_ (:workers opts)]
      (.start (Thread. disp)))
    ;; start lifecycle
    (.start (Thread. kill))
    ;; This becomes idxr above!
    (fn [bulk]
      (.put q bulk))))

(defn -main [& args]
  (let [[opts args _] (apply cli args opts)]
    (if (:version opts)
      (quit (version))
      (let [indexer (start-indexer-pool opts)
            state (ref (merge
                        opts
                        {:curr 0
                         :bytes 0
                         :indexer indexer
                         :items []}))
            parser (xml/make-parser (:url opts) (make-handler state))]
        (try
          (.parse parser)
          (catch Exception e
            (quit "can't parse: %s" (str e))))))))
