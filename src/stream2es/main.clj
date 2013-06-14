(ns stream2es.main
  (:gen-class)
  ;; Need to require these because of the multimethod in s.stream.
  (:require [stream2es.stream.wiki :as wiki]
            [stream2es.stream.stdin :as stdin]
            [stream2es.stream.twitter :as twitter])
  (:require [cheshire.core :as json]
            [clojure.tools.cli :refer [cli]]
            [stream2es.auth :as auth]
            [stream2es.log :as log]
            [stream2es.es :as es]
            [stream2es.size :refer [size-of]]
            [stream2es.version :refer [version]]
            [stream2es.stream :as stream]
            [stream2es.help :as help]
            [stream2es.util.io :as io]
            [stream2es.util.string :as s]
            [stream2es.util.time :as time]
            [stream2es.worker :as worker]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (clojure.lang ExceptionInfo)
           (java.io FileNotFoundException)
           (java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue
                                 TimeUnit)))

(def quit? true)

(def worker-threads
  (int (/ (.availableProcessors (Runtime/getRuntime)) 2)))

(def index-settings
  {:number_of_shards 2
   :number_of_replicas 0
   :refresh_interval -1})

(def opts
  [["--take" "Number of docs to index"
    :default -1
    :parse-fn #(Integer/parseInt %)]
   ["-q" "--queue" "Size of the internal bulk queue"
    :default 40
    :parse-fn #(Integer/parseInt %)]
   ["--stream-buffer" "Buffer up to this many pages"
    :default 50
    :parse-fn #(Integer/parseInt %)]
   ["--drop" "Skip this many docs before indexing"
    :default 0
    :parse-fn #(Integer/parseInt %)]
   ["-v" "--version" "Print version" :flag true :default false]
   ["-w" "--workers" "Number of indexing threads"
    :default worker-threads
    :parse-fn #(Integer/parseInt %)]
   ["--tee" "Save bulk request payloads as files in path"]
   ["--mappings" "Index mappings" :default nil]
   ["--settings" "Index settings" :default nil]
   ["--replace" "Delete index before streaming" :flag true :default false]
   ["--indexing" "Whether to actually send data to ES"
    :flag true :default true]
   ["--authinfo" "Stored stream credentials"
    :default (str
              (System/getProperty "user.home")
              (java.io.File/separator)
              ".authinfo.stream2es")]
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
          f (io/file path (str name ".gz"))]
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

(defn make-json-string [items]
  (->> items
       (map :source)
       (map json/encode)
       (interpose "\n")
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
        (let [first-id (-> bulk first :meta :index :_id)]
          (when (:indexing @state)
            (let [idxbulk (make-indexable-bulk bulk)
                  idxbulkbytes (count (.getBytes idxbulk))
                  bulk-bytes (reduce + (map #(get-in % [:source :bytes]) bulk))
                  url (format "%s/%s" (:es @state) "_bulk")]
              (es/post url idxbulk)
              (dosync
               (alter state update-in [:total :indexed :docs] + (count bulk))
               (alter state update-in [:total :indexed :bytes] + bulk-bytes)
               (alter state update-in [:total :indexed :wire-bytes]
                      + idxbulkbytes))
              (index-status first-id (count bulk) idxbulkbytes state))
            (log/debug "adding indexed total"
                       (get-in @state [:total :indexed :docs])
                       "+" (count bulk)))
          (when (:tee @state)
            (let [data (make-json-string bulk)]
              (spit-mkdirs (:tee @state) (str first-id ".json") data)))))
      (recur q state))))

(defn index-buffer [state]
  ;; handle all things bulk->ES related
  (log/info "worker" (:worker-id state) "would index here")
  )

(defn flush-work-buffer [state]
  (let [items (-> state :buf deref :items)
        bytes (-> state :buf deref :bytes)]
    (log/info (:worker-id state) 'flushing bytes 'bytes)
    (swap! (:buf state)
           (fn [old]
             (-> old
                 (assoc :bytes 0)
                 (assoc :items []))))
    state))

(defn flush-worker [state]
  (let [indexing? (-> state :opts :indexing)]
    (when indexing?
      (log/info 'flush-worker "would es/post")
      (index-buffer state))
    (flush-work-buffer state)))

(defn maybe-flush-worker [state]
  (let [max-bulk-bytes (-> state :opts :bulk-bytes)
        bytes (-> state :buf deref :bytes)]
    (when (>= bytes max-bulk-bytes)
      (flush-worker state))))

(defn make-processor [opts]
  (fn [state n obj]
    (when-let [source (stream/make-source obj)]
      (let [item (source2item (:index opts) (:type opts) n source)]
        (swap! (:buf state)
               (fn [old]
                 (-> old
                     (update-in [:bytes] + (-> source str .getBytes count))
                     (update-in [:items] conj item))))))
    (maybe-flush-worker state)))

(defn stop-streaming? [obj curr opts]
  (let [enough? (and
                 (pos? (:take opts))
                 (>= curr (+ (:drop opts 0) (:take opts))))]
    (or enough? (worker/nil-or-eof? obj))))

(defn stream! [opts]
  (let [publish (worker/make-queue
                 :name "worker"
                 :queue-size (:stream-buffer opts)
                 :workers (:workers opts)
                 :process (make-processor opts)
                 :notify (:notifier opts)
                 :stop-streaming? stop-streaming?
                 :opts opts
                 :finalize flush-worker)
        stream-runner (stream/make-runner (:stream opts) opts publish)]
    ((-> stream-runner :runner))))

(defn start! [opts]
  (let [printed-done? (atom false)
        stats (atom {})
        main-latch (CountDownLatch. 1)
        notifier (fn [uptime workers _stats]
                   (swap! stats merge _stats)
                   (.countDown main-latch))
        end (fn []
              (when-not @printed-done?
                (log/info
                 (format
                  "streamed %d indexed %d bytes xfer %d"
                  (-> @stats :streamed :docs)
                  (-> @stats :indexed :docs)
                  (-> @stats :indexed :wire-bytes)))
                (reset! printed-done? true)))
        done (fn []
               (log/debug "waiting for streamer")
               (.await main-latch)
               (end)
               ;; with a single worker this helps get all the output written
               ;; to the terminal
               (.sleep TimeUnit/MILLISECONDS 10)
               (quit))]
    (.start (Thread. done "lifecycle"))
    (.addShutdownHook
     (Runtime/getRuntime) (Thread. end "SIGTERM handler"))
    (merge opts
           {:started-at (System/currentTimeMillis)
            :notifier notifier
            :stats @stats})))

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
          settings (merge index-settings
                          (stream/settings stream)
                          (json/decode settings true))]
      (es/post es index (json/encode
                         {:settings settings
                          :mappings mappings})))))

(defn main [world]
  (let [opts (start! world)]
    (try
      (when (:indexing opts)
        (ensure-index opts))
      (log/info
       (format "stream %s%s"
               (:cmd opts) (if (:url opts)
                             (format " from %s" (:url opts))
                             "")))
      (when (:tee opts)
        (log/info (format "saving bulks to %s" (:tee opts))))
      (stream! opts)
      (catch Exception e
        (.printStackTrace e)
        (quit "stream error: %s" (str e))))))

(defn main-no-command [opts]
  (when (:help opts)
    (quit (help)))
  (when (:version opts)
    (quit (version))))

(defn main-with-command [args]
  (try+
    (let [[cmd stream] (get-stream args)
          main-plus-cmd-specs (concat opts (stream/specs stream))
          [optmap args _] (parse-opts args main-plus-cmd-specs)]
      (when (:help optmap)
        ;; user must want help for particular stream
        (quit (help stream)))
      (when (and (= cmd 'twitter) (:authorize optmap))
        (auth/store-creds (:authinfo optmap) (twitter/make-creds optmap))
        (quit "*** Success! Credentials saved to %s" (:authinfo optmap)))
      (main (assoc optmap :stream stream :cmd cmd)))
    (catch [:type :stream2es.auth/nocreds] _
      (quit (format "Error: %s" (:message &throw-context))))
    (catch [:type ::badcmd] _
      (quit (format "Error: %s\n\n%s" (:message &throw-context) (help))))
    (catch [:type ::badarg] _
      (let [msg (format "Error: %s\n\n%s" (:message &throw-context) (help))]
        (quit msg)))
    (catch Object _
      (let [t (:throwable &throw-context)]
        (.printStackTrace t)
        (quit "unexpected exception: %s"
              (str t))))))

(defn -main [& origargs]
  (try+
    (let [[opts args _] (parse-opts origargs opts)]
      (if (empty? args)
        (main-no-command opts)
        (main-with-command origargs)))
    (catch [:type ::badarg] _
      ;; check and make sure a command doesn't use it
      (main-with-command origargs))))
