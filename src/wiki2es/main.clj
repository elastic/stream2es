(ns wiki2es.main
  (:gen-class)
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [wiki2es.log :as log]
            [wiki2es.size :refer [size-of]]
            [wiki2es.version :refer [version]]
            [wiki2es.xml :as xml])
  (:import (java.util.concurrent LinkedBlockingQueue)))

(def _index
  "wiki")

(def _type
  "page")

(def queue-size
  10)

(def bulk-bytes
  (* 3 1024 1024))

(defrecord BulkItem [meta source])

(defn quit
  ([]
     (quit "done"))
  ([s]
     (quit "%s" s))
  ([fmt & s]
     (apply log/infof fmt s)
     (shutdown-agents)
     (System/exit 0)))

(defn page2item [offset page]
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
  (let [item (page2item (:curr @state) page)]
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
  (let [{:keys [skip stopafter curr]} @state]
    (if (pos? stopafter)
      (< curr (+ skip stopafter))
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
  (let [url (format "http://localhost:9200/%s/_bulk" _index)]
    (http/post url {:body data})))

(defn make-indexable-bulk [items]
  (->> (for [item items]
         (str (json/encode (:meta item))
              "\n"
              (json/encode (:source item))
              "\n"))
       (apply str)))

(defn index-bulk [q total]
  (let [bulk (.take q)]
    (when (sequential? bulk)
      (log/info "<--< pull bulk:" (count bulk) "items")
      (post (make-indexable-bulk bulk)))
    (when (= :stop bulk)
      (quit "processed %d docs" total))
    (recur q (+ total (count bulk)))))

(defn start-indexer []
  (let [q (LinkedBlockingQueue. queue-size)
        total-bulks (atom 0)
        thr (Thread. (fn [] (index-bulk q 0)))]
    (.start thr)
    ;; This becomes idxr above!
    (fn [bulk]
      (.put q bulk))))

(defn -main [& args]
  (let [[bz2 stopafter skip] args]
    (if bz2
      (let [indexer (start-indexer)
            state (ref {:stopafter (Integer/parseInt (or stopafter "-1"))
                        :maxbytes bulk-bytes
                        :skip (Integer/parseInt (or skip "0"))
                        :curr 0
                        :bytes 0
                        :indexer indexer
                        :items []})
            parser (xml/make-parser bz2 (make-handler state))]
        (.parse parser))
      (println "version" (version)))))
