(ns stream2es.test.main
  (:require [cheshire.core :as json]
            [clojure.test :refer :all]
            [stream2es.es]
            [stream2es.stream.twitter])
  (:require [stream2es.main] :reload))

(deftest help
  (with-redefs [stream2es.main/quit (fn [& args] (first args))
                stream2es.main/main (fn [_])]
    (testing "no args"
      (is (nil? (stream2es.main/-main))))
    (testing "good cmd"
      (is (nil? (stream2es.main/-main "stdin"))))
    (testing "single --help"
      (is (.startsWith (stream2es.main/-main "--help") "Copyright")))
    (testing "badcmd"
      (is (.contains (stream2es.main/-main "foo") "foo is not a")))))

(deftest index-settings
  (let [ops (atom [])
        stream (stream2es.stream.twitter.TwitterStream.)
        settings (json/encode stream2es.main/index-settings)]
    (with-redefs [stream2es.es/post (fn [_ _ payload]
                                      (swap! ops conj :post)
                                      payload)
                  stream2es.es/delete (fn [& _]
                                        (swap! ops conj :delete))
                  stream2es.es/exists? (fn [& _]
                                         (swap! ops conj :exists?)
                                         false)]
      (testing "use defaults"
        (is (= {:settings (json/decode settings true)
                :mappings {:thing
                           {:properties
                            {:location {:type "geo_point"}}}}}
               (json/decode
                (stream2es.main/ensure-index
                 {:replace false
                  :stream stream
                  :index "test"
                  :type "thing"
                  :settings settings})
                true))))

      (testing "merge defaults"
        (reset! ops [])
        (let [mappings (json/encode
                        {:thing
                         {:properties
                          {:location {:type "long"}}}})]
          (is (= {:settings stream2es.main/index-settings
                  :mappings {:thing
                             {:properties
                              {:location {:type "long"}}}}}
                 (json/decode
                  (stream2es.main/ensure-index
                   {:replace true
                    :stream stream
                    :index "test"
                    :type "thing"
                    :settings settings
                    :mappings mappings})
                  true))))))))
