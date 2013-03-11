# stream2es

For when you need a little more control than
[elasticsearch-river-wikipedia](https://github.com/elasticsearch/elasticsearch-river-wikipedia).

## Install

        % curl -O download.elasticsearch.org/stream2es/stream2es; chmod +x stream2es

## Quick Start

By default, `stream2es` indexes the latest Wikipedia article dump.

        % stream2es
        streaming wiki from http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
        00:08.260 17.1d/s 372.2K/s (141 docs 3148232 bytes 10)
        00:09.926 23.3d/s 631.9K/s (90 docs 3274854 bytes 661)
        00:11.769 28.4d/s 794.2K/s (103 docs 3148672 bytes 807)
        00:13.461 31.8d/s 926.0K/s (94 docs 3191976 bytes 959)
        00:14.630 35.7d/s 1065.1K/s (95 docs 3193050 bytes 1132)
        00:16.117 40.4d/s 1158.1K/s (128 docs 3155862 bytes 1274)
        00:17.433 44.5d/s 1247.7K/s (124 docs 3161022 bytes 1461)
        00:18.947 49.0d/s 1311.6K/s (154 docs 3173714 bytes 1629)
        00:20.506 50.8d/s 1365.9K/s (112 docs 3233356 bytes 1828)
        00:22.225 51.5d/s 1398.7K/s (103 docs 3152258 bytes 1980)
        00:23.708 52.8d/s 1441.2K/s (107 docs 3154382 bytes 2132)
        00:25.154 53.7d/s 1482.3K/s (101 docs 3192626 bytes 2289)
        00:26.944 57.7d/s 1497.9K/s (204 docs 3147292 bytes 2459)
        00:28.309 60.1d/s 1535.9K/s (145 docs 3195732 bytes 2729)
        00:29.651 62.3d/s 1571.8K/s (145 docs 3199920 bytes 2943)
        00:31.910 62.0d/s 1558.0K/s (132 docs 3184380 bytes 3163)
        00:33.319 63.0d/s 1584.9K/s (121 docs 3165818 bytes 3395)
        00:35.503 63.3d/s 1576.6K/s (147 docs 3243414 bytes 3640)
        00:37.942 61.7d/s 1557.1K/s (96 docs 3181124 bytes 3851)
        00:40.078 61.2d/s 1550.9K/s (110 docs 3150636 bytes 4032)
        00:42.086 60.4d/s 1550.3K/s (91 docs 3165122 bytes 4200)
        00:43.861 60.1d/s 1558.2K/s (93 docs 3171090 bytes 4352)
          C-c C-c
        streamed 2704 indexed docs 2636 indexed bytes 69984532

If you're at a café or want to use a local copy of the dump, supply `--url`:

        % ./stream2es wiki --max-docs 5 --url /d/data/enwiki-20121201-pages-articles.xml.bz2

stream2es streams into a buffer and publishes bulk requests in a
queue.  Indexing threads pull those bulk requests concurrently.  A
page callback may block if it has to wait for a spot.  The log lines
with arrows refer to a bulk request being published on the indexing
queue, and removed when ES has acknowledged receipt.

## Options

        Copyright 2013 Elasticsearch
        
        Usage: stream2es [CMD] [OPTS]
        
        Available commands: wiki, twitter, stdin
        
        Common opts:
        -u --es         ES location (default: "http://localhost:9200")
        -h --help       Display help (default: false)
           --mappings   Index mappings (default: null)
        -d --max-docs   Number of docs to index (default: -1)
           --replace    Delete index before streaming (default: false)
           --settings   Index settings (default: "{"number_of_replicas":0,"refresh_interval":-1,"number_of_shards":2}")
        -s --skip       Skip this many docs before indexing (default: 0)
           --tee        Save bulk request payloads as files in path (default: null)
        -v --version    Print version (default: false)
        -w --workers    Number of indexing threads (default: 2)
        
        TwitterStream opts:
        -b --bulk-bytes Bulk size in bytes (default: 102400)
        -i --index      ES index (default: "twitter")
           --pass       Twitter password (default: "")
        -q --queue      Size of the internal bulk queue (default: 1000)
           --stream-buffer Buffer up to this many tweets (default: 1000)
        -t --type       ES document type (default: "status")
           --user       Twitter username (default: "")
        
        StdinStream opts:
        -b --bulk-bytes Bulk size in bytes (default: 102400)
        -i --index      ES index (default: "foo")
        -q --queue      Size of the internal bulk queue (default: 40)
           --stream-buffer Buffer up to this many docs (default: 100)
        -t --type       ES type (default: "t")
        
        WikiStream opts:
        -b --bulk-bytes Bulk size in bytes (default: 3145728)
        -i --index      ES index (default: "wiki")
        -q --queue      Size of the internal bulk queue (default: 40)
           --stream-buffer Buffer up to this many pages (default: 50)
        -t --type       ES type (default: "page")
        -u --url        Wiki dump locator (default: "http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2")

By default, the `refresh_interval` is `-1`, which lets ES refresh as
it needs to.  You can change it by supplying custom `--settings`:

        % echo '{"name":"alfredo"}' | ./stream2es stdin --settings '
        {
            "settings": {
                "refresh_interval": "2m"
            }
        }'


# Contributing

stream2es is written in Clojure.  You'll need leiningen 2.0+ to build.

        % lein bin
        % target/stream2es

# License

This software is licensed under the Apache 2 license, quoted below.

        Copyright 2009-2013 Elasticsearch <http://www.elasticsearch.org>

        Licensed under the Apache License, Version 2.0 (the "License"); you may not
        use this file except in compliance with the License. You may obtain a copy of
        the License at

            http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
        WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
        License for the specific language governing permissions and limitations under
        the License.
