# wiki2es

For when you need a little more control than
[elasticsearch-river-wikipedia](https://github.com/elasticsearch/elasticsearch-river-wikipedia).

## Install

        % curl -O download.elasticsearch.org/wiki2es/wiki2es; chmod +x wiki2es

## Quick Start

By default, `wiki2es` indexes 500 pages from the latest article dump.

        % ./wiki2es
        >--> push bulk: items:141 bytes:3169452 first-id:10
        <--< pull bulk: 141 items
        >--> push bulk: items:90 bytes:3263022 first-id:661
        <--< pull bulk: 90 items
        <--< pull bulk: 105 items
        >--> push bulk: items:105 bytes:3169114 first-id:807
        >--> push bulk: items:93 bytes:3220726 first-id:963
        <--< pull bulk: 93 items
        >--> push bulk: items:71 bytes:2273986 first-id:1134
        <--< pull bulk: 71 items
        processed 500 docs

Index 100 Wikipedia docs *starting at* document 100.

        % ./wiki2es --max-docs 100 --skip 100
        >--> push bulk: items:91 bytes:3164018 first-id:593
        <--< pull bulk: 91 items
        <--< pull bulk: 9 items
        >--> push bulk: items:9 bytes:345440 first-id:740
        processed 100 docs

If you're at a café or want to use a local copy of the dump, supply `--url`:

        % ./wiki2es --max-docs 5 --url /d/data/enwiki-20121201-pages-articles.xml.bz2
        <--< pull bulk: 5 items
        >--> push bulk: items:5 bytes:109426 first-id:10
        processed 5 docs

What's this push/pull output?  wiki2es starts up indexing threads that
pull bulk requests concurrently from an internal queue.  A page
callback may block if it has to wait for a spot.

The line `>--> push bulk: items:92 bytes:3153144 first-id:593` means
that a bulk request with 92 docs, of roughly 3153144 bytes in size,
with first doc ID of 593 has been enqueued.  `<--< pull bulk: 92 items`
indicates that the bulk request has been pulled from the queue
and has been submitted for indexing to ES.

The bulk size defaults to 3MiB but you can supply `--bulk-bytes` to
change it.

## Options

        --index         ES index (default: wiki)
        --bulk-bytes    Bulk size in bytes (default: 3145728)
        --max-docs      Number of docs to index (default: 500)
        --queue         Size of the internal bulk queue (default: 20)
        --skip          Skip this many docs before indexing (default: 0)
        --type          ES type (default: page)
        --url           Wiki dump locator (default: http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2)
        --version       Print version (default: false)
        --workers       Number of indexing threads (default: 2)

I strongly suggest increasing the `refresh_interval` to get better
indexing performance.

        curl -s -XPUT localhost:9200/wiki/_settings -d '{"refresh_interval":"2m"}'; echo


# Contributing

wiki2es is written in Clojure.  You'll need leiningen 2.0+ to build.

        % lein bin
        % target/wiki2es /path/to/dump

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
