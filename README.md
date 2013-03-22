# stream2es

For when you need a little more control than
[elasticsearch-river-wikipedia](https://github.com/elasticsearch/elasticsearch-river-wikipedia).

## Install

        % curl -O download.elasticsearch.org/stream2es/stream2es; chmod +x stream2es

## Quick Start

By default, `stream2es` reads JSON documents from stdin.

        % echo '{"foo":1}' | stream2es
        create index foo
        stream stdin
        flushing index queue
        00:00.505 2.0d/s 0.1K/s 1 1 70
        streamed 1 docs 1 bytes xfer 70
        %

You can also index the latest Wikipedia article dump.

        % stream2es wiki --index tmp            
        create index tmp
        stream wiki from http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-art
        icles.xml.bz2
        00:04.984 44.7d/s 622.7K/s 223 223 3177989 10
        00:07.448 54.1d/s 838.1K/s 403 180 3213889 794
        00:09.715 63.0d/s 961.4K/s 612 209 3172256 1081
        00:12.000 73.2d/s 1036.1K/s 878 266 3167860 1404
        00:14.385 75.2d/s 1079.9K/s 1082 204 3174907 1756
        ^Cstreamed 1158 docs 1082 bytes xfer 15906901

What is the output telling me?

        00:12.000: MM:SS that the app has been running
          73.2d/s: Docs per second indexed
        1036.1K/s: Bytes per second indexed (bulk transferred over the wire)
              878: Total docs indexed so far
              266: Docs in bulk
          3167860: Total JSON bytes of docs in the bulk
             1404: The _id of the first doc in the bulk

If you're at a café or want to use a local copy of the dump, supply `--url`:

        % ./stream2es wiki --max-docs 5 --url /d/data/enwiki-20121201-pages-articles.xml.bz2

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
