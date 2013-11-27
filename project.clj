(defproject org.elasticsearch/stream2es
  (try (-> "etc/version.txt" slurp .trim)
       (catch java.io.FileNotFoundException _
         (println "WARNING! Missing etc/version.txt,"
                  "falling back to 0.0.1-SNAPSHOT")
         "0.0.1-SNAPSHOT"))
  :description "Index streams into ES."
  :url "http://github.com/elasticsearch/elasticsearch/stream2es"
  :license {:name "Apache 2"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :resource-paths ["etc" "resources"]
  :dependencies [[cheshire "5.0.1"]
                 [clj-http "0.6.3"]
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.2"]
                 [org.elasticsearch/elasticsearch-river-wikipedia "1.2.0"]
                 [org.twitter4j/twitter4j-stream "3.0.3"]
                 [slingshot "0.10.3"]
                 [clj-oauth "1.4.0"]
                 [org.tukaani/xz "1.3"]
                 [org.elasticsearch/workroom "0.99.0"]]
  :plugins [[lein-bin "0.3.2"]]
  :aot [stream2es.main]
  :main stream2es.main
  :bin {:bootclasspath true})
