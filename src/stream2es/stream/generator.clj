(ns stream2es.stream.generator
  (:refer-clojure :exclude [name type])
  (:require [cheshire.core :as json]
            [clojure.java.io :as jio]
            [clojure.string :refer [split]]
            [stream2es.es :as es]
            [stream2es.http :as http]
            [stream2es.log :as log]
            [stream2es.util.string :refer [rand-str]]
            [stream2es.stream :refer [new specs Stream
                                      Streamable CommandLine
                                      StreamStorage]]))

(defrecord GeneratorStream [])

(defrecord GeneratorStreamRunner [runner])

(defmethod new 'generator [_]
  (GeneratorStream.))

(defprotocol FieldSpec
  (parse [spec]))

(defprotocol Field
  (make [field opts]))

(defrecord IntField [name size]
  Field
  (make [field opts]
    (map->IntField
     {:type :int
      :name name
      :size size
      :value (rand-int size)})))

(defrecord StringField [name size]
  Field
  (make [field opts]
    (let [v (rand-str (:size field) (::dictionary opts))]
      (map->StringField
       (assoc field :value v )))))

(defrecord DoubleField [name size]
  Field
  (make [field opts]
    (let [v (Double/parseDouble
             (format "%d.%d" (rand-int size) (rand-int 1000000)))]
      (map->DoubleField
       {:type :double
        :name name
        :size size
        :value v}))))

(extend-protocol FieldSpec
  String
  (parse [spec]
    (when (pos? (count (.trim spec)))
      (let [[name type size] (split spec #":")
            size (try (Integer/parseInt size) (catch Exception _ 0))]
        (case type
          "int" (IntField. name size)
          "integer" (IntField. name size)
          "str" (StringField. name size)
          "string" (StringField. name size)
          "dbl" (DoubleField. name size)
          "double" (DoubleField. name size)
          (StringField. name size))))))

(defn parse-fields [template]
  (filter identity (map parse (split template #","))))

(defn make-doc-map [fields opts]
  (let [xs (map #(make % opts) fields)
        kvs (map #(vector (:name %) (:value %)) xs)]
    (into {} kvs)))

(defn make-doc [fields opts]
  (json/encode (make-doc-map fields opts)))

(defn get-dictionary [location]
  (if location
    (vec (line-seq (jio/reader location)))
    []))

(extend-type GeneratorStream
  CommandLine
  (specs [_]
    [["-b" "--bulk-bytes" "Bulk size in bytes"
      :default (* 1024 100)
      :parse-fn #(Integer/parseInt %)]
     ["-q" "--queue-size" "Size of the internal bulk queue"
      :default 40
      :parse-fn #(Integer/parseInt %)]
     ["--target" "ES index" :default "http://localhost:9200/foo/t"]
     ["--dictionary" "Dictionary location" :default "/usr/share/dict/words"]
     ["--fields" "Field template (str, string, dbl, double, int, integer)"
      :default "f1:str:1"]
     ["--stream-buffer" "Buffer up to this many docs"
      :default 100000
      :parse-fn #(Integer/parseInt %)]])
  Stream
  (bootstrap [_ opts]
    (let [dict (get-dictionary (:dictionary opts))
          fields (parse-fields (:fields opts))]
      (merge opts
             {::fields fields
              ::dictionary dict})))
  (make-runner [_ opts handler]
    (GeneratorStreamRunner.
     (fn []
       (dotimes [_ (:workers opts)]
         (.start
          (Thread.
           (fn []
             (handler (make-doc (::fields opts) opts))
             (recur))))))))
  StreamStorage
  (settings [_ opts]
    {:number_of_shards 2
     :number_of_replicas 0})
  (mappings [_ opts]
    {(keyword (-> opts :target es/type-name))
     {:_all {:enabled false}
      :properties {}}}))
