# wiki2es

For when you need a little more control than the
`elasticsearch-river-wikipedia` gives you.

## Install

        % curl -O download.elasticsearch.org/wiki2es/wiki2es; chmod +x wiki2es

## Usage

Index 100 Wikipedia docs.

        % wiki2es /path/to/enwiki-20121201-pages-articles.xml.bz2 100
        >--> push bulk: items:100 bytes:1747760 first-id:10
        <--< pull bulk: 100 items
        processed 100 docs
        %

Index 100 Wikipedia docs *starting at* document 100.

        % wiki2es /d/data/enwiki-20121201-pages-articles.xml.bz2 100 100
        >--> push bulk: items:92 bytes:3153144 first-id:593
        <--< pull bulk: 92 items
        >--> push bulk: items:8 bytes:314410 first-id:742
        <--< pull bulk: 8 items
        processed 100 docs
        %

Currently the bulk size is hard-coded at 3MiB.

# Contributing

es2unix is written in Clojure.  You'll need leiningen 2.0+ to build.

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
