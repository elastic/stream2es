(ns stream2es.main
  (:gen-class)
  ;; Need to require these because of the multimethod in s.stream.
  (:require [stream2es.stream.wiki :as wiki]
            [stream2es.stream.stdin :as stdin]
            [stream2es.stream.twitter :as twitter])
  (:require [cheshire.core :as json]
            [clojure.tools.cli :refer [cli]]
            [clojure.tools.logging :as log]
            [stream2es.es :as es]
            [stream2es.size :refer [size-of]]
            [stream2es.version :refer [version]]
            [stream2es.stream :as stream]
            [stream2es.help :as help]
            [stream2es.util.io :as io]
            [stream2es.util.string :as s]
            [stream2es.util.time :as time]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (clojure.lang ExceptionInfo)
           (java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue)))

(def quit? true)

(def indexing-threads 2)

(def index-settings
  {:number_of_shards 2
   :number_of_replicas 0
   :refresh_interval -1})

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
   ["--tee" "Save bulk request payloads as files in path"]
   ["--mappings" "Index mappings" :default nil]
   ["--settings" "Index settings" :default (json/encode index-settings)]
   ["--replace" "Delete index before streaming" :flag true :default false]
   ["--indexing" "Whether to actually send data to ES"
    :flag true :default true]
   ["-u" "--es" "ES location" :default "http://localhost:9200"]
   ["-h" "--help" "Display help" :flag true :default false]])

(defrecord BulkItem [meta source])

(defn quit
  ([]
     (quit ""))
  ([s]
     (quit "%s" s))
  ([fmt & s]
     (when (pos? (count (first s)))
       (println (apply format fmt s)))
     (when quit?
       (shutdown-agents)
       (System/exit 0))))

(defn source2item [_index _type offset source]
  (let [bytes (-> source json/encode .getBytes count)]
    (BulkItem.
     {:index
      (merge
       {:_index _index
        :_type _type}
       (when (:_id source)
         {:_id (str (:_id source))}))}
     (merge (dissoc source :_id)
            {:bytes bytes
             :offset offset}))))

(defn flush-bulk [state]
  (let [itemct (count (:items @state))
        items (:items @state)]
    (when (pos? itemct)
      #_(log/info
         (format ">--> %d items; %d bytes; first-id %s"
                 itemct (:bytes @state)
                 (-> items first :meta :index :_id)))
      ((:indexer @state) items)
      (dosync
       (alter state assoc :bytes 0)
       (alter state assoc :items [])))))

(defn maybe-index [state]
  (let [{:keys [bytes bulk-bytes]} @state]
    (when (> bytes bulk-bytes)
      (flush-bulk state))))

(defn continue? [state]
  (let [curr (get-in @state [:total :streamed :docs])
        {:keys [skip max-docs]} @state]
    (log/trace 'skip skip 'max-docs max-docs 'curr curr)
    (if (pos? max-docs)
      (< curr (+ skip max-docs))
      true)))

(defn skip? [state]
  (>= (:skip @state) (get-in @state [:total :streamed :docs])))

(defn flush-indexer [state]
  (log/info "flushing index queue")
  (dotimes [_ (:workers @state)]
    ((:indexer @state) :stop)))

(defn want-shutdown [state]
  (log/debug "want shutdown")
  (flush-bulk state)
  (flush-indexer state))

(defn spit-mkdirs [path name data]
  (when path
    (let [sub (s/hash-dir 2)
          path (io/file path sub)
          f (io/file path name)]
      (log/debug "save" (str f) (count (.getBytes data)) "bytes")
      (.mkdirs (io/file path))
      (io/spit-gz f data))))

(defn make-indexable-bulk [items]
  (->> (for [item items]
         (str (json/encode (:meta item))
              "\n"
              (json/encode (:source item))
              "\n"))
       (apply str)))

(defn index-status [id bulk-count bulk-bytes state]
  (let [upmillis (- (System/currentTimeMillis) (:started-at @state))
        upsecs (float (/ upmillis 1e3))
        index-doc-rate (/ (get-in @state [:total :indexed :docs]) upsecs)
        index-kbyte-rate (/
                          (/ (get-in @state [:total :indexed :wire-bytes]) 1024)
                          upsecs)
        #_stream-doc-rate #_(/ (get-in @state [:total :streamed :docs]) upsecs)
        #_stream-kbyte-rate #_(/
                               (/ (get-in @state [:total :streamed :bytes])
                                  1024)
                               upsecs)]
    (log/info
     (format "%s %.1fd/s %.1fK/s %d %d %d%s"
             (time/minsecs upsecs)
             index-doc-rate index-kbyte-rate
             (get-in @state [:total :indexed :docs])
             bulk-count bulk-bytes
             (if id (format " %s" id) "")))))

(defn index-bulk [q state]
  (let [bulk (.take q)]
    (when-not (= :stop bulk)
      (when (and (sequential? bulk) (pos? (count bulk)))
        (let [first-id (-> bulk first :meta :index :_id)
              idxbulk (make-indexable-bulk bulk)
              idxbulkbytes (count (.getBytes idxbulk))
              bulk-bytes (reduce + (map #(get-in % [:source :bytes]) bulk))
              url (format "%s/%s" (:es @state) "_bulk")]
          (when (:indexing @state)
            (es/post url idxbulk))
          (dosync
           (alter state update-in [:total :indexed :docs] + (count bulk))
           (alter state update-in [:total :indexed :bytes] + bulk-bytes)
           (alter state update-in [:total :indexed :wire-bytes] + idxbulkbytes))
          (index-status first-id (count bulk) idxbulkbytes state)
          (spit-mkdirs
           (:tee @state)
           (str first-id ".bulk.gz")
           idxbulk)))
      (log/debug "adding indexed total"
                 (get-in @state [:total :indexed :docs]) "+" (count bulk))
      (recur q state))))

(defn start-indexer-pool [state]
  (let [q (LinkedBlockingQueue. (:queue @state))
        latch (CountDownLatch. (:workers @state))
        disp (fn []
               (index-bulk q state)
               (log/debug "waiting for POSTs to finish")
               (.countDown latch))
        lifecycle (fn []
                    (.await latch)
                    (log/debug "done indexing")
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
        stop (fn []
               (want-shutdown state)
               (.countDown latch))
        disp (fn []
               (let [obj (.take q)]
                 (if-not (and (not (= :eof obj))
                              (continue? state))
                   (stop)
                   (do
                     (dosync
                      (alter state update-in [:total :streamed :docs] inc))
                     (when-not (skip? state)
                       (process obj)
                       (maybe-index state))
                     (recur)))))
        lifecycle (fn []
                    (.await latch)
                    (log/debug "done collecting")
                    ((:collector-notifier @state)))]
    (.start (Thread. disp "stream dispatcher"))
    (.start (Thread. lifecycle "stream service"))
    ;; publisher
    (fn [stream-object]
      (.put q stream-object))))

(defn make-object-processor [state]
  (fn [stream-object]
    (log/trace 'stream-object stream-object)
    (let [source (stream/make-source stream-object)]
      (dosync
       (let [item (source2item
                   (:index @state)
                   (:type @state)
                   (get-in @state [:total :streamed :docs])
                   source)]
         (alter state update-in
                [:bytes] + (-> item :source :bytes))
         (alter state update-in
                [:total :streamed :bytes]
                + (-> source str .getBytes count))
         (alter state update-in
                [:items] conj item))))))

(defn stream! [state]
  (let [process (make-object-processor state)
        publish (start-doc-stream state process)
        stream-runner (stream/make-runner (:stream @state) @state publish)]
    ((-> stream-runner :runner))))

(defn start! [opts]
  (let [state (ref
               (merge opts
                      {:started-at (System/currentTimeMillis)
                       :bytes 0
                       :items []
                       :total {:indexed {:docs 0
                                         :bytes 0
                                         :wire-bytes 0}
                               :streamed {:docs 0
                                          :bytes 0}}}))
        collector-latch (CountDownLatch. 1)
        indexer-latch (CountDownLatch. 1)
        collector-notifier #(.countDown collector-latch)
        indexer-notifier #(.countDown indexer-latch)
        indexer (start-indexer-pool state)
        printed-done? (atom false)
        end (fn []
              (when-not @printed-done?
                (log/info
                 (format
                  "streamed %d indexed %d bytes xfer %d"
                  (-> @state :total :streamed :docs)
                  (-> @state :total :indexed :docs)
                  (-> @state :total :indexed :wire-bytes)))
                (reset! printed-done? true)))
        done (fn []
               (log/debug "waiting for collectors")
               (.await collector-latch)
               (log/debug "waiting for indexers")
               (.await indexer-latch)
               (end)
               (quit))]
    (.start (Thread. done "lifecycle"))
    (.addShutdownHook
     (Runtime/getRuntime) (Thread. end "SIGTERM handler"))
    (dosync
     (alter state assoc :collector-notifier collector-notifier)
     (alter state assoc :indexer-notifier indexer-notifier)
     (alter state assoc :indexer indexer))
    state))

(defn parse-opts [args specs]
  (try
    (apply cli args specs)
    (catch Exception e
      (throw+ {:type ::badarg} (.getMessage e)))))

(defn help-preamble []
  (with-out-str
    (println "Copyright 2013 Elasticsearch")
    (println)
    (println "Usage: stream2es [CMD] [OPTS]")
    (println)
    (println "Available commands: wiki, twitter, stdin")
    (println)
    (println "Common opts:")
    (print (help/help opts))))

(defn help-stream [& streams]
  (with-out-str
    (doseq [stream streams :let [inst (if (satisfies? stream/CommandLine stream)
                                        stream
                                        (.newInstance stream))]]
      (println)
      (println (format "%s opts:"
                       (second (re-find #"\.([^.]+)@0$" (str inst)))))
      (print (help/help (stream/specs inst))))))

(defn help [& streams]
  (with-out-str
    (print (help-preamble))
    (print
     (apply help-stream
            (if (seq streams)
              streams
              (extenders stream/CommandLine))))))

(defn get-stream [args]
  (let [cmd (if (seq args)
              (let [tok (first args)]
                (when (.startsWith tok "-")
                  (throw+ {:type ::badarg} ""))
                (symbol tok))
              'stdin)]
    (try
      [cmd (stream/new cmd)]
      (catch IllegalArgumentException _
        (throw+ {:type ::badcmd}
                "%s is not a valid command" cmd)))))

(defn ensure-index [{:keys [stream es index type
                            mappings settings replace]}]
  (when replace
    (log/info "delete index" index)
    (es/delete es index))
  (when-not (es/exists? es index)
    (log/info "create index" index)
    (let [mappings (merge (stream/mappings stream type)
                          (json/decode mappings true))
          settings (merge (stream/settings stream)
                          (json/decode settings true))]
      (es/post es index (json/encode
                         {:settings settings
                          :mappings mappings})))))

(defn main [world]
  (let [state (start! world)]
    (try
      (when (:indexing @state)
        (ensure-index @state))
      (log/info
       (format "stream %s%s"
               (:cmd @state) (if (:url @state)
                               (format " from %s" (:url @state))
                               "")))
      (when (:tee @state)
        (log/info (format "saving bulks to %s" (:tee @state))))
      (stream! state)
      (catch Exception e
        (.printStackTrace e)
        (quit "stream error: %s" (str e))))))

(defn -main [& args]
  (try+
    (let [[cmd stream] (get-stream args)
          main-plus-cmd-specs (concat opts (stream/specs stream))
          [optmap args _] (parse-opts args main-plus-cmd-specs)]
      (when (:help optmap)
        (quit (help stream)))
      (if (:version optmap)
        (quit (version))
        (main (assoc optmap :stream stream :cmd cmd))))
    (catch [:type ::badcmd] _
      (quit (format "Error: %s\n\n%s" (:message &throw-context) (help))))
    (catch [:type ::badarg] _
      (let [msg (format "%s%s" (:message &throw-context) (help))]
        (quit msg)))
    (catch Object _
      (let [t (:throwable &throw-context)]
        (.printStackTrace t)
        (quit "unexpected exception: %s"
              (str t))))))
