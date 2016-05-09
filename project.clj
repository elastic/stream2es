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
  :dependencies [[cheshire "5.6.1"]
                 [clj-http "3.0.1"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.elasticsearch/elasticsearch-river-wikipedia "2.6.0"]
                 [org.twitter4j/twitter4j-stream "3.0.5"]
                 [slingshot "0.12.2"]
                 [clj-oauth "1.5.5"]
                 [org.tukaani/xz "1.5"]
                 [com.taoensso/timbre "4.3.1"]
                 [org.clojure/core.typed "0.3.23"]]
  :plugins [[lein-bin "0.3.2"]]
  :aot :all
  :main stream2es.main
  :bin {:bootclasspath true}
  :core.typed {:check [stream2es.http]})
