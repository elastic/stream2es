(ns stream2es.main
  (:gen-class)
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.tools.cli :refer [cli]]
            [clojure.tools.logging :as log2]
            [stream2es.log :as log]
            [stream2es.size :refer [size-of]]
            [stream2es.version :refer [version]]
            [stream2es.wiki :as wiki]
            [stream2es.twitter :as twitter]
            [stream2es.help :as help]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue)))

(def indexing-threads
  2)

(def opts
  [["-d" "--max-docs" "Number of docs to index"
    :default -1
    :parse-fn #(Integer/parseInt %)]
   ["-s" "--skip" "Skip this many docs before indexing"
    :default 0
    :parse-fn #(Integer/parseInt %)]
   ["-v" "--version" "Print version" :flag true :default false]
   ["-w" "--workers" "Number of indexing threads"
    :default indexing-threads
    :parse-fn #(Integer/parseInt %)]
   ["-h" "--help" "Display help" :flag true :default false]])

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

(defn source2item [_index _type offset source]
  (BulkItem.
   {:index
    {:_index _index
     :_type _type
     :_id (:_id source)}}
   (merge (dissoc source :_id)
          {:bytes (size-of source)
           :offset offset})))

(defn flush-bulk [state]
  (dosync
   (let [itemct (count (:items @state))
         items (:items @state)]
     (when (pos? itemct)
       (log2/info
        (format ">--> %d items; %d bytes; first-id %s"
                itemct (:bytes @state)
                (-> items first :meta :index :_id)))
       ((:indexer @state) items)
       (alter state assoc :bytes 0)
       (alter state assoc :items [])))))

(defn maybe-index [state]
  (let [{:keys [bytes bulk-bytes]} @state]
    (when (> bytes bulk-bytes)
      (flush-bulk state))))

(defn continue? [{:keys [skip max-docs curr]}]
  (log2/trace 'skip skip 'max-docs max-docs 'curr curr)
  (if (pos? max-docs)
    (< curr (+ skip max-docs))
    true))

(defn flush-indexer [state]
  (log2/info "flushing index queue")
  (dotimes [_ (:workers @state)]
    ((:indexer @state) :stop)))

(defn want-shutdown [state]
  (log2/debug "want shutdown")
  (flush-bulk state)
  (flush-indexer state))

(defn post [data]
  (log2/trace "POSTing" (count data) "bytes")
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
    (when-not (= :stop bulk)
      (when (and (sequential? bulk) (pos? (count bulk)))
        (log2/info "<--<" (count bulk)
                   "items; first-id" (-> bulk first :meta :index :_id))
        (post (make-indexable-bulk bulk)))
      (log2/debug "adding indexed total" @total "+" (count bulk))
      (swap! total + (count bulk))
      (recur q total))))

(defn start-indexer-pool [state]
  (let [q (LinkedBlockingQueue. (:queue @state))
        latch (CountDownLatch. (:workers @state))
        total (atom 0)
        disp (fn []
               (index-bulk q total)
               (log2/debug "waiting for POSTs to finish")
               (.countDown latch))
        lifecycle (fn []
                    (.await latch)
                    (dosync
                     (alter state update-in [:total :indexed] + @total))
                    (log2/debug "done indexing")
                    ((:indexer-notifier @state)))]
    ;; start index pool
    (dotimes [n (:workers @state)]
      (.start (Thread. disp (str "indexer " (inc n)))))
    ;; notify when done
    (.start (Thread. lifecycle "index service"))
    ;; This becomes :indexer above!
    (fn [bulk]
      (.put q bulk))))

(defn start-doc-stream [state process]
  (let [q (LinkedBlockingQueue. (:stream-buffer @state))
        latch (CountDownLatch. 1)
        disp (fn []
               (let [obj (.take q)]
                 (if-not (continue? @state)
                   (do
                     (want-shutdown state)
                     (.countDown latch))
                   (do
                     (dosync
                      (alter state update-in [:curr] inc)
                      (when (> (:curr @state) (:skip @state))
                        (process obj)
                        (maybe-index state)))
                     (recur)))))
        lifecycle (fn []
                    (.await latch)
                    (log2/debug "done collecting")
                    ((:collector-notifier @state)))]
    (.start (Thread. disp "stream dispatcher"))
    (.start (Thread. lifecycle "stream service"))
    ;; publisher
    (fn [stream-object]
      (.put q stream-object))))

(defn make-object-processor [state make-source]
  (fn [stream-object]
    (let [source (make-source stream-object)]
      (dosync
       (let [item (source2item
                   (:index @state)
                   (:type @state)
                   (:curr @state)
                   source)]
         (alter state update-in
                [:bytes] + (-> item :source :bytes))
         (alter state update-in
                [:items] conj item))))))

(defn stream! [state]
  (let [make-source (condp = (:cmd @state)
                      'twitter twitter/make-source
                      'wiki wiki/make-source)
        process (make-object-processor state make-source)
        publish (start-doc-stream state process)
        stream-handler (condp = (:cmd @state)
                         'twitter (twitter/make-stream
                                   (:user @state)
                                   (:pass @state)
                                   publish)
                         'wiki (wiki/make-stream (:url @state) publish))]
    ((:run stream-handler) stream-handler)))

(defn start! [opts]
  (let [state (ref
               (merge opts
                      {:curr 0
                       :bytes 0
                       :items []
                       :total {:indexed 0}}))
        collector-latch (CountDownLatch. 1)
        indexer-latch (CountDownLatch. 1)
        collector-notifier #(.countDown collector-latch)
        indexer-notifier #(.countDown indexer-latch)
        indexer (start-indexer-pool state)
        kill (fn []
               (log2/debug "waiting for collectors")
               (.await collector-latch)
               (log2/debug "waiting for indexers")
               (.await indexer-latch)
               (quit "streamed %d indexed %d"
                     (-> @state :curr)
                     (-> @state :total :indexed)))]
    (.start (Thread. kill "lifecycle"))
    (dosync
     (alter state assoc :collector-notifier collector-notifier)
     (alter state assoc :indexer-notifier indexer-notifier)
     (alter state assoc :indexer indexer))
    state))

(defn cmd-specs [cmd]
  (try
    (->> (format "stream2es.%s/%s" cmd 'opts)
         symbol
         find-var
         deref)
    (catch Exception _
      (throw+ {:type ::badcmd} "Can't find options for command %s" cmd))))

(defn parse-opts [args specs]
  (try
    (apply cli args specs)
    (catch Exception e
      (throw+ {:type ::badarg} (.getMessage e)))))

(defn help []
  (with-out-str
    (println "Copyright 2013 Elasticsearch")
    (println)
    (println "Usage: stream2es [CMD] [OPTS]")
    (println)
    (println "Available commands: wiki, twitter")
    (println)
    (println "Common opts:")
    (println (help/help opts))
    (println "Wikipedia opts (default):")
    (println (help/help wiki/opts))
    (println "Twitter opts:")
    (print (help/help twitter/opts))))

(defn -main [& args]
  (try+
   (let [cmd (symbol (or (first args) 'wiki))
         main-plus-cmd-specs (concat opts (cmd-specs cmd))
         [optmap args _] (parse-opts args main-plus-cmd-specs)]
     (when (:help optmap)
       (quit (help)))
     (if (:version optmap)
       (quit (version))
       (let [state (start! (assoc optmap
                             :cmd cmd))]
         (try
           (stream! state)
           (catch Exception e
             (quit "stream error: %s" (str e)))))))
   (catch [:type ::badcmd] _
     (quit (help)))
   (catch [:type ::badarg] _
     (let [msg (format "%s\n\n%s" (:message &throw-context) (help))]
       (quit msg)))
   (catch Object _
     (quit "unexpected exception: %s"
           (:throwable &throw-context)))))
