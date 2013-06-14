(ns stream2es.worker
  (:require [stream2es.log :as log])
  (:import (java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue
                                 TimeUnit)))

(def stream-wait  ; secs
  10)

(def poison
  :kill)

(defn kill-workers [q n]
  (log/info "killing" n "workers")
  (dotimes [_ n]
    (.put q poison)))

(defn poison? [obj]
  (condp = obj
    poison true
    nil true
    false))

(defn nil-or-eof? [obj]
  (condp = obj
    nil true
    :eof true
    false))

(defn poll
  "Worker id polls q in intervals waiting for an object to appear.  It
   waits for a maximum of wait seconds."

  [id q wait]
  (let [interval 2
        retry-count (int (/ wait interval))]
    (loop [remaining retry-count]
      (when (< (/ remaining retry-count) 0.30)
        (log/info "worker" id "waiting for stream..." remaining))
      (when (pos? remaining)
        (if-let [obj (.poll q interval TimeUnit/SECONDS)]
          obj
          (recur (dec remaining)))))))

(defn make-queue*
  [args]
  (let [{:keys [name

                stop-streaming?         ; pred to determine whether to
                                        ; keep streaming

                enqueue?                ; pred to determing whether
                                        ; obj should be enqueued

                queue-size              ; how many msgs to buffer

                workers                 ; number of worker threads

                process                 ; fn that gets called with
                                        ; local state and queue obj

                process?                ; predicate to determine
                                        ; whether to process the
                                        ; current msg

                finalize                ; not getting any more
                                        ; messages, so run this
                                        ; (useful for flushing buffers

                notify                  ; fn called when consumer
                                        ; finishes

                init                    ; initial local worker state

                timeout                 ; how long before giving up on
                                        ; stream

                opts                    ; optional persistent config
                ]
         :or {init {}
              timeout stream-wait
              opts {}
              process? (fn [& args] true)
              enqueue? (fn [& args] true)
              queue-size 10
              stop-streaming? (fn [obj curr opts]
                                (nil-or-eof? obj))
              finalize (fn [& args])}}
        args

        start (System/currentTimeMillis)
        q (LinkedBlockingQueue. workers)
        latch (CountDownLatch. workers)
        dead? #(not (= (.getCount latch) workers)) ;; at least one worker died
        totals (atom {:streamed {:docs 0}})
        publish (fn [obj]
                  (if (dead?)
                    :dead
                    (if (stop-streaming?
                         obj
                         (get-in @totals [:streamed :docs]) opts)
                      (kill-workers q workers)
                      (do
                        (swap! totals update-in [:streamed :docs] inc)
                        (when (enqueue?
                               obj
                               (get-in @totals [:streamed :docs]) opts)
                          (if-not (.offer q obj 5 TimeUnit/SECONDS)
                            (log/info "waiting for space"
                                      "to enqueue stream object...")))))))
        work (fn [state]
               (fn []
                 (loop []
                   (let [obj (poll (:worker-id state) q timeout)]
                     (if (or (poison? obj) (dead?))
                       (finalize state)
                       (do
                         (when (process? state
                                         (get-in @totals
                                                 [:streamed :docs]))
                           (process state (get-in
                                           @totals [:streamed :docs]) obj)
                           (let [bytes (-> obj str .getBytes count)]
                             (swap!
                              totals
                              update-in [:bytes (:worker-id state)]
                              (fnil + 0) (-> obj str .getBytes count))
                             (swap!
                              totals
                              update-in [:bytes :all]
                              (fnil + 0) (-> obj str .getBytes count))
                             (swap!
                              totals
                              update-in [:processed (:worker-id state)]
                              (fnil inc 0))
                             (swap!
                              totals
                              update-in [:processed :all]
                              (fnil inc 0))))
                         (recur)))))
                 (log/info "worker" (:worker-id state) "done")
                 (.countDown latch)))
        lifecycle (fn []
                    (log/info "waiting for" workers "workers")
                    (.await latch)
                    (log/info "all workers done")
                    (notify (- (System/currentTimeMillis) start)
                            workers
                            @totals))]
    (dotimes [n workers]
      (.start
       (Thread. (work (assoc init
                        :opts opts
                        :worker-id n
                        :buf (atom {:items []
                                    :bytes 0})
                        :bytes-indexed (atom 0)
                        :stats (atom {})
                        :status ..............))
                (format "%s-%d" name (inc n)))))
    (.start (Thread. lifecycle (format "%s service" name)))
    publish))

(defn make-queue [& {:as args}]
  (make-queue* args))
