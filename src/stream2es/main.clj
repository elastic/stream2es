(ns stream2es.main
  (:gen-class)
  ;; Need to require these because of the multimethod in s.stream.
  (:require [cheshire.core :as json]
            [clojure.tools.cli :refer [cli]]
            [slingshot.slingshot :refer [try+ throw+]]
            [stream2es.bootstrap :refer [boot help]]
            [stream2es.es :as es]
            [stream2es.log :as log]
            [stream2es.opts :as opts]
            [stream2es.util.rate :as rate]
            [stream2es.size :refer [size-of]]
            [stream2es.stream :as stream]
            [stream2es.util.io :as io]
            [stream2es.util.string :as s]
            [stream2es.util.stacktrace :as stack]
            [stream2es.version :refer [version]])
  (:import (java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue
                                 TimeUnit)))

(def quit? true)

(defrecord BulkItem [meta source])

(defn quit
  ([code s]
   (quit code "%s" s))
  ([code fmt & s]
   (if (pos? code)
     (log/error (apply format fmt s))
     (log/info (apply format fmt s)))
   (when quit?
     (log/flush)
     (shutdown-agents)
     (System/exit code))))

(defn source2item [_type offset store-offset? source]
  (let [s2e-meta (:__s2e_meta__ source)]
    (BulkItem.
     {:index
      (merge
       {:_type (:_type s2e-meta _type)}
       (when (:_id s2e-meta)
         {:_id (str (:_id s2e-meta))})
       (when (:_routing s2e-meta)
         {:_routing (:_routing s2e-meta)})
       (when (:_parent s2e-meta)
         {:_parent (:_parent s2e-meta)}))
      :_bytes (-> source json/encode .getBytes count)}
     (merge (dissoc source :__s2e_meta__)
            (when store-offset?
              {opts/offset-field offset})))))

(defn flush-bulk [state]
  (let [itemct (count (:items @state))
        items (:items @state)]
    (when (pos? itemct)
      (log/trace
       (format ">--> flush-bulk %d items; %d bytes; first-id %s"
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
    (if (pos? max-docs)
      (< curr (+ skip max-docs))
      true)))

(defn skip? [state]
  (>= (:skip @state) (get-in @state [:total :streamed :docs])))

(defn flush-indexer [state]
  (log/trace "flushing index queue")
  (dotimes [_ (:workers @state)]
    ((:indexer @state) :stop)))

(defn want-shutdown [state]
  (log/trace "want shutdown")
  (flush-bulk state)
  (flush-indexer state))

(defn spit-mkdirs [path name data]
  (when path
    (let [sub (s/hash-dir 2)
          path (io/file path sub)
          f (io/file path (str name ".gz"))]
      (log/trace "save" (str f) (count (.getBytes data)) "bytes")
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
  (let [replace-id (fn [item]
                     (merge (:source item)
                            {:_id (-> item :meta :index :_id)}
                            (when (-> item :meta :index :_type)
                              {:_type (-> item :meta :index :_type)})))]
    (->> items
         (map replace-id)
         (map json/encode)
         (interpose "\n")
         (apply str))))

(defn index-status [id bulk-count bulk-bytes state]
  (let [rate (rate/calc
              (:started-at @state)
              (System/currentTimeMillis)
              (get-in @state [:total :indexed :docs])
              (get-in @state [:total :indexed :wire-bytes]))]
    (log/debug
     (format "%s %.1fd/s %.1fK/s %d %d %d %d%s"
             (rate :minsecs)
             (rate :docs-per-sec)
             (rate :kb-per-sec)
             (get-in @state [:total :indexed :docs])
             bulk-count bulk-bytes
             (get-in @state [:total :errors])
             (if id (format " %s" id) "")))))

(defn index-bulk [q state]
  (let [bulk (.take q)]
    (when-not (= :stop bulk)
      (when (and (sequential? bulk) (pos? (count bulk)))
        (let [first-id (-> bulk first :meta :index :_id)
              idxbulk (make-indexable-bulk bulk)
              idxbulkbytes (count (.getBytes idxbulk))]
          (let [bulk-bytes (reduce + (map #(get-in % [:meta :_bytes]) bulk))
                errors (when (:indexing @state)
                         (es/error-capturing-bulk (:target @state) bulk
                                                  make-indexable-bulk)
                         (dosync
                          (alter state update-in [:total :indexed :docs]
                                 + (count bulk))
                          (alter state update-in [:total :indexed :bytes]
                                 + bulk-bytes)
                          (alter state update-in [:total :indexed :wire-bytes]
                                 + idxbulkbytes))
                         (log/trace "adding indexed total"
                                    (get-in @state [:total :indexed :docs])
                                    "+" (count bulk)))]
            (dosync
             (alter state update-in [:total :errors]
                    (fnil + 0) (or errors 0)))
            (index-status first-id (count bulk) idxbulkbytes state))
          (when (:tee-bulk @state)
            (spit-mkdirs
             (:tee-bulk @state) (str first-id ".bulk") idxbulk))
          (when (:tee @state)
            (let [data (make-json-string bulk)]
              (spit-mkdirs (:tee @state) (str first-id ".json") data)))))
      (recur q state))))

(defn start-indexer-pool [state]
  (let [q (LinkedBlockingQueue. (:queue-size @state))
        latch (CountDownLatch. (:workers @state))
        disp (fn []
               (index-bulk q state)
               (log/trace "waiting for POSTs to finish")
               (.countDown latch))
        lifecycle (fn []
                    (.await latch)
                    (log/trace "done indexing")
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
               (let [obj (if (pos? (:stream-timeout @state))
                           (.poll q (:stream-timeout @state)
                                  TimeUnit/SECONDS)
                           (.take q))]
                 (if-not (and obj
                              (not (= :eof obj))
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
                    (log/trace "done collecting")
                    ((:collector-notifier @state)))]
    (.start (Thread. disp "stream dispatcher"))
    (.start (Thread. lifecycle "stream service"))
    ;; publisher
    (fn [stream-object]
      (.put q stream-object))))

(defn make-object-processor [state]
  (fn [stream-object]
    (let [source (stream/make-source stream-object @state)]
      (when source
        (dosync
         (let [type (es/type-name (:target @state))
               item (source2item type
                                 (get-in @state [:total :streamed :docs])
                                 (:offset @state)
                                 source)]
           (alter state update-in
                  [:bytes] + (-> item :meta :_bytes))
           (alter state update-in
                  [:total :streamed :bytes]
                  + (-> source str .getBytes count))
           (alter state update-in
                  [:items] conj item)))))))

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
                       :total {:errors 0
                               :indexed {:docs 0
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
                (let [rate (rate/calc
                            (:started-at @state)
                            (System/currentTimeMillis)
                            (-> @state :total :indexed :docs)
                            (-> @state :total :indexed :wire-bytes))]
                  (log/info
                   (format
                    (str "%s %.1fd/s %.1fK/s (%.1fmb) indexed %d "
                         "streamed %d errors %d")
                    (rate :minsecs)
                    (rate :docs-per-sec)
                    (rate :kb-per-sec)
                    (rate :mb)
                    (rate :docs)
                    (-> @state :total :streamed :docs)
                    (-> @state :total :errors))))
                (reset! printed-done? true)))
        done (fn []
               (log/trace "waiting for collectors")
               (.await collector-latch)
               (log/trace "waiting for indexers")
               (.await indexer-latch)
               (end)
               (if (pos? (-> @state :total :errors))
                 (quit 1 (format "finished with %d errors"
                                 (-> @state :total :errors)))
                 (quit 0 "done")))]
    (.start (Thread. done "lifecycle"))
    (.addShutdownHook
     (Runtime/getRuntime) (Thread. end "SIGTERM handler"))
    (dosync
     (alter state assoc :collector-notifier collector-notifier)
     (alter state assoc :indexer-notifier indexer-notifier)
     (alter state assoc :indexer indexer))
    state))

(defn ensure-index [{:keys [stream target mappings settings replace]
                     :as opts}]
  (when replace
    (es/delete-index target))
  (when-not (es/index-exists? target)
    (log/debug "create index" (es/index-url target))
    (let [mappings (merge (stream/mappings stream opts)
                          (json/decode mappings true))
          settings (merge (stream/settings stream opts)
                          (json/decode settings true))]
      (es/put-index target (json/encode
                            {:settings settings
                             :mappings mappings})))))

(defn main [world]
  (let [state (start! world)]
    (try
      (when (:indexing @state)
        (ensure-index @state)
        (log/debug
         (format "stream %s%sto %s"
                 (:cmd @state)
                 (if (:source @state)
                   (format " from %s " (.url (:source @state)))
                   " ")
                 (.url (:target @state)))))
      (when (:tee-bulk @state)
        (log/debug (format "saving bulks to %s" (:tee-bulk @state))))
      (when (:tee @state)
        (log/debug (format "saving json to %s" (:tee @state))))
      (stream! state)
      (catch java.net.ConnectException e
        (throw+ {:type ::network} "%s connection refused" (:target @state))))))

(defn -main [& args]
  (try+
   (main (boot args))
   (catch [:type :authorized] _
     (quit 0 (:message &throw-context)))
   (catch [:type :help] {:keys [msg]}
     (quit 0 (or msg (help))))
   (catch [:type :version] _
     (quit 0 (format "stream2es %s" (version))))
   (catch [:type :stream2es.auth/nocreds] _
     (quit 11 (format "Error: %s" (:message &throw-context))))
   (catch [:type :stream2es.bootstrap/badcmd] _
     (quit 12 (format "badcmd: %s\n\n%s" (:message &throw-context) (help))))
   (catch [:type :stream2es.opts/badarg] _
     (quit 13 (format "badarg: %s\n\n%s" (:message &throw-context) (help))))
   (catch [:type ::network] _
     (quit 14 (format "Network error: %s" (:message &throw-context))))
   (catch [:type :stream-death] {:keys [msg]}
     (quit 15 (format "stream terminated: %s" msg)))
   (catch [:type :stream-invalid-args] {:keys [msg stream]}
     (quit 16 (format "%s args invalid: %s" stream msg)))
   (catch [:type :bad-null-bad] {:keys [where]}
     (quit 98 (format "bad unhandled NULL around [[ %s ]]:\n\n%s"
                      where
                      (stack/stacktrace
                       (:throwable &throw-context)))))
   (catch Object _
     (let [t (:throwable &throw-context)]
       (.printStackTrace t)
       (quit 99 "unexpected exception: see above")))))
