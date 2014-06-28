(ns stream2es.test.main
  (:require [cheshire.core :as json]
            [clojure.test :refer :all]
            [stream2es.es]
            [stream2es.stream]
            [stream2es.auth :as auth]
            [stream2es.stream.twitter :as twitter])
  (:require [stream2es.main] :reload))

(defn noop [& args])

(deftest help
  (with-redefs [stream2es.main/quit (fn [& args] (first args))
                stream2es.main/main noop
                auth/store-creds noop
                twitter/make-creds noop]
    (testing "no args"
      (is (nil? (stream2es.main/-main))))
    (testing "good cmd"
      (is (nil? (stream2es.main/-main "stdin"))))
    (testing "--help"
      (is (= 0 (stream2es.main/-main "--help"))))
    (testing "twitter --authorize"
      (is (= 0 (stream2es.main/-main "twitter" "--authorize"))))
    (testing "badcmd"
      (is (= 12 (stream2es.main/-main "foo"))))))

(deftest index-settings
  (let [ops (atom [])
        stream (stream2es.stream/new 'stdin)
        opts {:stream stream
              :target "http://localhost:9200/foo/t"
              :mappings (json/encode {:thing
                                      {:_all {:enabled false}
                                       :properties
                                       {:location {:type "geo_point"}}}})}]
    (with-redefs [stream2es.es/post (fn [_ _ payload]
                                      (swap! ops conj :post)
                                      payload)
                  stream2es.es/put (fn [_ payload]
                                     (swap! ops conj :put)
                                     payload)
                  stream2es.es/delete (fn [& _]
                                        (swap! ops conj :delete))
                  stream2es.es/exists? (fn [& _]
                                         (swap! ops conj :exists?)
                                         false)]
      (testing "use defaults"
        (is (= {:settings
                {:number_of_replicas 0
                 :number_of_shards 2
                 :index.refresh_interval "5s"
                 :index.number_of_shards 2
                 :index.number_of_replicas 0}
                :mappings {:thing
                           {:_all {:enabled false}
                            :properties {:location {:type "geo_point"}}}
                           :t {:_all {:enabled false}, :properties {}}}}
               (json/decode
                (stream2es.main/ensure-index opts)
                true)))
        (is (= @ops [:exists? :put]))))))
