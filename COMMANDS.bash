#!/usr/bin/env bash

# Copy configuration of nutch-site.xml to runtime/local
cp conf/nutch-site.xml runtime/local/conf/nutch-site.xml
cp conf/regex-urlfilter.txt runtime/local/conf/regex-urlfilter.txt

rm -Rf crawled

runtime/local/bin/crawl -i urls crawled 1

rm -Rf pages && runtime/local/bin/nutch readdb crawled/crawldb -dump pages/1 -format csv && mv pages/1/part-00000 pages/1/part-00000.csv.txt

#EOL


