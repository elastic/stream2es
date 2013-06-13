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

(defn make-queue
  [& {:keys [continue?  ; pred applied to current msg
                                        ; count to determine whether
                                        ; to enqueue next object

             queue-size ; how many msgs to buffer

             workers    ; number of worker threads

             process    ; fn that gets called with
                                        ; local state and queue obj

             notify     ; fn called when consumer
                                        ; finishes

             init       ; initial local worker state

             timeout    ; how long before giving up on
                                        ; stream
             ]
      :or {init (atom {})
           timeout stream-wait}}]
  (let [init (if (instance? clojure.lang.Atom init)
               init
               (atom init))
        start (System/currentTimeMillis)
        q (LinkedBlockingQueue. workers)
        latch (CountDownLatch. workers)
        alive? #(= (.getCount latch) workers)
        other-dead-workers? #(not alive?)
        totals (atom {:total {:streamed {:docs 0}}})
        publish (fn [obj]
                  (if (alive?)
                    (if (continue?
                         obj
                         (get-in @totals [:total :streamed :docs]))
                      (if (.offer q obj 5 TimeUnit/SECONDS)
                        (swap! totals update-in [:total :streamed :docs] inc)
                        (log/info "waiting for space"
                                  "to enqueue stream object..."))
                      (kill-workers q workers))
                    :dead))
        work (fn [state]
               (fn []
                 (loop []
                   (let [obj (poll (:worker-id @state) q timeout)]
                     (process state (if (poison? obj) nil obj))
                     (when-not (or (poison? obj)
                                   (other-dead-workers?))
                       (swap!
                        totals
                        update-in [:total :processed (:worker-id @state)]
                        (fnil inc 0))
                       (swap!
                        totals
                        update-in [:total :processed :all]
                        (fnil inc 0))
                       (recur))))
                 #_(log/info "worker" (:worker-id @state) "done")
                 (.countDown latch)))
        lifecycle (fn []
                    (.await latch)
                    (log/info "all workers done")
                    (notify (- (System/currentTimeMillis) start)
                            workers
                            @totals))]
    (dotimes [n workers]
      (.start
       (Thread. (work (atom (merge @init {:worker-id n})))
                (format "%s-%d" name (inc n)))))
    (.start (Thread. lifecycle (format "%s service" name)))
    publish))

(comment
  (let [max-objs -1
        flush-queue? #(>= (count (:items %)) (:bulk-count %))]
    (def enqueue
      (make-queue :workers 3
                  :process (fn [state obj]
                             (when obj
                               (swap! state update-in [:items] conj obj))
                             (when (or (not obj)
                                       (flush-queue? @state))
                               #_(log/info "worker"
                                           (:worker-id @state)
                                           "processed"
                                           (count (:items @state))
                                           "items!")
                               (swap! state assoc-in [:items] [])
                               )
                             #_(log/info @state obj))
                  :continue? (fn [obj n]
                               (let [enough? (and
                                              (pos? max-objs)
                                              (>= n max-objs))
                                     stop? (condp = obj
                                             nil true
                                             :eof true
                                             false)]
                                 (not (or stop? enough?))))
                  :queue-size 1000
                  :notify (fn [uptime workers stats]
                            (log/info "notify"
                                      (int (/ uptime 1000))
                                      workers
                                      stats))
                  :init {:bulk-count 5}
                  :timeout 30
                  )))


  )
