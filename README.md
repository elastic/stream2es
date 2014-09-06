# stream2es

Standalone utility to stream different inputs into Elasticsearch.

## Read This First

*If you've just wandered here, first check out [Logstash](http://github.com/elasticsearch/logstash).  It's a much more general tool, and one of our featured products.  If for some reason it doesn't do something that's important to you, create an issue there.  stream2es is mostly a quick hack to ingest a bit of data so you can get working with ES.  We will fix bugs as we have time.*

## Install

You'll need Java 7.  Run `java -version` to make sure.

Then download `stream2es`:

    % curl -O download.elasticsearch.org/stream2es/stream2es; chmod +x stream2es

# Usage

## stdin

By default, `stream2es` reads JSON documents from stdin.

    % echo '{"field":1}' | stream2es
    create index http://localhost:9200/foo
    stream stdin
    flushing index queue
    00:00.505 2.0d/s 0.1K/s 1 1 70
    streamed 1 docs 1 bytes xfer 70
    %

## Generator

`stream2es` can fuzz data for you.  It can create blank documents, or documents with integer fields, or documents with string fields if you supply a dictionary.

Blank documents are easy:

```
stream2es generator
```

Ints need to know how big you want them.  This template would give you a single field with values between `0` and `127`, inclusive.

```
stream2es generator --fields "f1:int:128"
```

To add a string, we need to add a template for it, and a file of newline-separated lines of text.  Given a field template of "NAME:str:N`stream2es` will select `N` number of words from each dictionary:

```
# zsh
% stream2es generator --fields "f1:int:128,f2:str:2" --dictionary <(echo foo\\nbar\\nbaz)
# any shell
% cat <<EOF >/tmp/dict
foo
bar
baz
EOF
% stream2es generator --fields "f1:int:128,f2:str:2" --dictionary /tmp/dict --max-docs 5
create index http://localhost:9200/foo
stream generator to http://localhost:9200/foo/t
flushing index queue
00:00.279 17.9d/s 1.1K/s 5 5 305 0
streamed 5 indexed 5 bytes xfer 305 errors 0
done
% curl localhost:9200/foo/_search\?format=yaml | fgrep -A2 _source
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

Fortunately, most *nix systems come with `/usr/share/dict/words`, which is a great choice if you just need some English text.  Linux distributions also have other packages you can install.


## Wikipedia

Index the latest Wikipedia article dump.

    % stream2es wiki --target http://localhost:9200/tmp
    create index http://localhost:9200/tmp
    stream wiki from http://download.wikimedia.org/enwiki/latest/enwiki-latest-pages-art
    icles.xml.bz2
    00:04.984 44.7d/s 622.7K/s 223 223 3177989 0 10
    00:07.448 54.1d/s 838.1K/s 403 180 3213889 0 794
    00:09.715 63.0d/s 961.4K/s 612 209 3172256 0 1081
    00:12.000 73.2d/s 1036.1K/s 878 266 3167860 0 1404
    00:14.385 75.2d/s 1079.9K/s 1082 204 3174907 0 1756
    ^Cstreamed 1158 docs 1082 bytes xfer 15906901 errors 0

What is the output telling me?

    00:12.000: MM:SS that the app has been running
      73.2d/s: Docs per second indexed
    1036.1K/s: Bytes per second indexed (bulk transferred over the wire)
          878: Total docs indexed so far
          266: Docs in bulk
      3167860: Total JSON bytes of docs in the bulk
            0: Total docs that have had indexing errors so far
         1404: The _id of the first doc in the bulk

If you're at a café or want to use a local copy of the dump, supply `--source`:

    % ./stream2es wiki --max-docs 5 --source /d/data/enwiki-20121201-pages-articles.xml.bz2

## Elasticsearch

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
