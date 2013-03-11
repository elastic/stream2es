(defproject org.elasticsearch/stream2es
  (-> "etc/version.txt" slurp .trim)
  :description "Index streams into ES."
  :url "http://github.com/elasticsearch/elasticsearch/stream2es"
  :license {:name "Apache 2"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :resource-paths ["etc" "resources"]
  :dependencies [[cheshire "5.0.1"]
                 [clj-http "0.6.3"]
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.2"]
                 [org.clojure/tools.logging "0.2.6"]
                 [org.elasticsearch/elasticsearch-river-wikipedia
                  "1.2.0-SNAPSHOT"]
                 [org.twitter4j/twitter4j-stream "3.0.3"]
                 [slingshot "0.10.3"]]
  :plugins [[lein-bin "0.3.2"]]
  :main stream2es.main
  :bin {:bootclasspath true})
