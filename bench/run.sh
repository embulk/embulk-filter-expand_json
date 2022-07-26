#!/usr/bin/env bash

BENCH_ROOT=$(cd $(dirname $0); pwd)
DATA_FILE=data.jsonl
TMP_DATA_FILE=tmp.jsonl

function now() {
    date +"%FT%T%:z"
}

echo "[$(now)] Preparing ..."
(
    cd $BENCH_ROOT
    embulk bundle

    if [ -f $DATA_FILE ]; then
        rm -f $DATA_FILE
    fi
    if [ -f $TMP_DATA_FILE ]; then
        rm -f $TMP_DATA_FILE
    fi
    for n in {1..100}; do
        cat ../example/data.tsv | cut -f5 >> $TMP_DATA_FILE
    done
    for n in {1..1000}; do
        cat $TMP_DATA_FILE >> $DATA_FILE
    done
)

echo "[$(now)] Run No expand_json"
(
    cd $BENCH_ROOT
    time embulk run -I ../build/gemContents/lib -b . config_raw.yml
)

echo "[$(now)] Run Default (LRUCache)"
(
    cd $BENCH_ROOT
    time embulk run -I ../build/gemContents/lib -b . config_with_lru_cache.yml
)

echo "[$(now)] Run with NOOPCache"
(
    cd $BENCH_ROOT
    time embulk run -I ../build/gemContents/lib -b . config_with_noop_cache.yml
)

echo "[$(now)] Teardown..."
(
    cd $BENCH_ROOT
    if [ -f $DATA_FILE ]; then
        rm -f $DATA_FILE
    fi
    if [ -f $TMP_DATA_FILE ]; then
        rm -f $TMP_DATA_FILE
    fi
)
