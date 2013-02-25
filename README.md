# stream2es

For when you need a little more control than
[elasticsearch-river-wikipedia](https://github.com/elasticsearch/elasticsearch-river-wikipedia).

## Install

        % curl -O download.elasticsearch.org/stream2es/stream2es; chmod +x stream2es

## Quick Start

By default, `stream2es` indexes the latest Wikipedia article dump.

        % ./stream2es
        streaming wiki from http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
        <--< 141 items; first-id 10
        >--> 141 items; 3149564 bytes; first-id 10
        <--< 90 items; first-id 661
        >--> 90 items; 3274342 bytes; first-id 661
        >--> 103 items; 3146938 bytes; first-id 807
        <--< 103 items; first-id 807
        >--> 94 items; 3195832 bytes; first-id 959
        <--< 94 items; first-id 959
        >--> 95 items; 3180746 bytes; first-id 1132
        <--< 95 items; first-id 1132


Index 100 Wikipedia docs *starting at* document 100.

        ./stream2es wiki --max-docs 100 --skip 100
        streaming wiki from http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
        >--> 91 items; 3182370 bytes; first-id 593
        <--< 91 items; first-id 593
        <--< 9 items; first-id 740
        >--> 9 items; 348052 bytes; first-id 740
        flushing index queue
        streamed 200 indexed 100

If you're at a café or want to use a local copy of the dump, supply `--url`:

        % ./stream2es wiki --max-docs 5 --url /d/data/enwiki-20121201-pages-articles.xml.bz2

stream2es streams into a buffer and publishes bulk requests in a
queue.  Indexing threads pull those bulk requests concurrently.  A
page callback may block if it has to wait for a spot.  The log lines
with arrows refer to a bulk request being published on the indexing
queue, and removed when ES has acknowledged receipt.

## Options

        Usage: stream2es [CMD] [OPTS]
        
        Available commands: wiki, twitter
        
        Common opts:
        -h --help       Display help (default: false)
        -d --max-docs   Number of docs to index (default: -1)
        -s --skip       Skip this many docs before indexing (default: 0)
        -v --version    Print version (default: false)
        -w --workers    Number of indexing threads (default: 2)
        
        Wikipedia opts (default):
        -b --bulk-bytes Bulk size in bytes (default: 3145728)
        -i --index      ES index (default: "wiki")
        -q --queue      Size of the internal bulk queue (default: 40)
           --stream-buffer Buffer up to this many tweets (default: 1000)
        -t --type       ES type (default: "page")
        -u --url        Wiki dump locator (default: "http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2")
        
        Twitter opts:
        -b --bulk-bytes Bulk size in bytes (default: 102400)
        -i --index      ES index (default: "twitter")
           --pass       Twitter password (default: "")
        -q --queue      Size of the internal bulk queue (default: 1000)
           --stream-buffer Buffer up to this many tweets (default: 1000)
        -t --type       ES document type (default: "status")
           --user       Twitter username (default: "")

I strongly suggest increasing the `refresh_interval` to get better
indexing performance.

        curl -s -XPUT localhost:9200/wiki/_settings -d '{"refresh_interval":"2m"}'; echo


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
