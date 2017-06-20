# stream2es

Standalone utility to stream different inputs into Elasticsearch.

## Read This First

*If you've just wandered here, first check out [Logstash](http://github.com/elasticsearch/logstash).  It's a much more general tool, and one of our featured products.  If for some reason it doesn't do something that's important to you, create an issue there.  stream2es is a dev tool that originated before the author knew much about Logstash.  That said, there are some important differences that are specific to Elasticsearch.  stream2es supports bulks by byte-length (`--bulk-bytes`) instead of doc count, which is crucial with docs of varying size.  It also supports exporting raw bulks via `--tee-bulk` to a hashed dir on the filesystem, and you can make the incoming stream finite with `--max-docs`.*

## Install

You'll need Java 8+.  Run `java -version` to make sure.

### Unix

Download `stream2es` and make it executable:

```
% curl -O download.elasticsearch.org/stream2es/stream2es; chmod +x stream2es
```

### Windows

```
> curl -O download.elasticsearch.org/stream2es/stream2es
> java -jar stream2es help
```



# Usage

## stdin

By default, `stream2es` reads JSON documents from stdin.

```
% echo '{"f":1}' | stream2es
2014-10-08T12:29:56.318-0500 INFO  00:00.116 8.6d/s 0.4K/s (0.0mb) indexed 1 streamed 1 errors 0
%
```

If you want more logging, set `--log debug`.  If you don't want any output, set `--log warn`.

## Wikipedia

Index the latest Wikipedia article dump.

    % stream2es wiki --target http://localhost:9200/tmp --log debug
    create index http://localhost:9200/tmp
    stream wiki from http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
    ^Cstreamed 1158 docs 1082 bytes xfer 15906901 errors 0

If you're at a café or want to use a local copy of the dump, supply `--source`:

    % ./stream2es wiki --max-docs 5 --source /d/data/enwiki-20121201-pages-articles.xml.bz2

Note that if you live-stream the WMF-hosted dump, it will cut off after a while. Grab a torrent and index it locally if you need more than a few thousand docs.

## Generator

`stream2es` can fuzz data for you.  It can create blank documents, or documents with integer fields, or documents with string fields if you supply a dictionary.

Blank documents are easy:

```
stream2es generator
```

Ints need to know how big you want them.  This template would give you a single field with values between `0` and `127`, inclusive.

```
stream2es generator --fields f1:int:128
```

To add a string, we need to add a template for it, and a file of newline-separated lines of text.  Given a field template of `NAME:str:N`, `stream2es` will select `N` random words from the dictionary for each field.

```
# zsh
% stream2es generator --fields f1:int:128,f2:str:2 --dictionary <(/bin/echo -e "foo\nbar\nbaz")
#### same as:
% stream2es generator --fields f1:int:128,f2:str:2 --dictionary /dev/stdin --max-docs 5 <<EOF
foo
bar
baz
EOF
% curl -s localhost:9200/foo/_search\?format=yaml | fgrep -A2 _source
    _source:
      f1: 28
      f2: "foo baz"
--
    _source:
      f1: 88
      f2: "baz foo"
--
    _source:
      f1: 26
      f2: "baz baz"
--
    _source:
      f1: 68
      f2: "bar baz"
--
    _source:
      f1: 64
      f2: "foo foo"
%
```

Fortunately, most *nix systems come with `/usr/share/dict/words` (Ubuntu package `wamerican-small`, for example), which is a great choice if you just need some (English) text.  Install other langs if you prefer.


## Elasticsearch

*Note: ES 2.3 added a [reindex API](https://www.elastic.co/guide/en/elasticsearch/reference/5.4/docs-reindex.html) that completely obviates this feature of stream2es.  Also, Logstash 1.5.0 has an [Elasticsearch input](https://www.elastic.co/guide/en/logstash/current/plugins-inputs-elasticsearch.html).*

If you use the `es` stream, you can copy indices from one Elasticsearch to another.  Example:

    % stream2es es \
         --source http://foo.local:9200/wiki \
         --target http://bar.local:9200/wiki2

This is a convenient way to reindex data if you need to change the number of shards or update your mapping.

## Twitter

In order to stream Twitter, you have to create an app and authorize it.

### Create app

Visit (https://dev.twitter.com/apps/new) and create an app.  Call it `stream2es`.  Note the `Consumer key` and `Consumer secret`.

### Authorize app

Now run `stream2es twitter --authorize --key CONSUMER_KEY --secret CONSUMER_SECRET` and complete the dialog.

### Run with new creds

You should now be able to stream twitter with simply `stream2es twitter`.  stream2es will grab the most recent cached credentials from `~/.authinfo.stream2es`.

### Tracking keywords

By default, stream2es will stream random sample of all public tweets, however
you can configure stream2es to _track_ specific keywords as follows:

    	stream2es twitter --track "Linux%%New York%%March Madness"

# Options

    % stream2es --help
    Copyright 2013 Elasticsearch

    Usage: stream2es [CMD] [OPTS]

    ..........


You can change index settings by supplying `--settings`:

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

You'll also need this little git alias if you want to do `make`:

```
[alias]
ver = "!git log --pretty=format:'%ai %h' -1 | perl -pe 's,(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d) (\\d\\d):(\\d\\d):(\\d\\d) [^ ]+ ([a-z0-9]+),\\1\\2\\3\\7,'"
```

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
