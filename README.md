# Expand Json filter plugin for Embulk

[![Release CI Status Badge](https://github.com/civitaspo/embulk-filter-expand_json/workflows/Release%20CI/badge.svg)](https://github.com/civitaspo/embulk-filter-expand_json/actions?query=workflow%3A%22Release+CI%22) [![Test CI Status Badge](https://github.com/civitaspo/embulk-filter-expand_json/workflows/Test%20CI/badge.svg)](https://github.com/civitaspo/embulk-filter-expand_json/actions?query=workflow%3A%22Test+CI%22)
[![Coverage Status](https://coveralls.io/repos/civitaspo/embulk-filter-expand_json/badge.svg?branch=main&service=github)](https://coveralls.io/github/civitaspo/embulk-filter-expand_json?branch=main)

expand columns having json into multiple columns

## Overview

* **Plugin type**: filter

## Configuration

- **json_column_name**: a column name having json to be expanded (string, required)
- **root**: root property to start fetching each entries, specify in [JsonPath](http://goessner.net/articles/JsonPath/) style (string, default: `"$."`)
- **expanded_columns**: columns expanded into multiple columns (array of hash, required)
  - **name**: name of the column. you can define [JsonPath](http://goessner.net/articles/JsonPath/) style.
  - **type**: type of the column (see below)
  - **format**: format of the timestamp if type is timestamp
  - **timezone**: Time zone of each timestamp columns if values don’t include time zone description (`UTC` by default)
- **keep_expanding_json_column**: Not remove the expanding json column from input schema if it's true (false by default)
- **default_timezone**: Time zone of timestamp columns if values don’t include time zone description (`UTC` by default)
- **stop_on_invalid_record**: Stop bulk load transaction if an invalid record is included (false by default)
- **cache_provider**: Cache provider name for JsonPath. `"LRU"` and `"NOOP"` are built-in. You can specify user defined class. (string, default: `"LRU"`)
  - `"NOOP"` becomes default in the future.

---
**type of the column**

|name|description|
|:---|:---|
|boolean|true or false|
|long|64-bit signed integers|
|timestamp|Date and time with nano-seconds precision|
|double|64-bit floating point numbers|
|string|Strings|


## Example

```yaml
filters:
  - type: expand_json
    json_column_name: json_payload
    root: "$."
    expanded_columns:
      - {name: "phone_numbers", type: string}
      - {name: "app_id", type: long}
      - {name: "point", type: double}
      - {name: "created_at", type: timestamp, format: "%Y-%m-%d", timezone: "UTC"}
      - {name: "profile.anniversary.et", type: string}
      - {name: "profile.anniversary.voluptatem", type: string}
      - {name: "profile.like_words[1]", type: string}
      - {name: "profile.like_words[2]", type: string}
      - {name: "profile.like_words[0]", type: string}
```


## Note
- If the value evaluated by JsonPath is Array or Hash, the value is written as JSON.

## Dependencies
- https://github.com/jayway/JsonPath
  - use to evaluate [JsonPath](http://goessner.net/articles/JsonPath/)
  - [Apache License Version 2.0](https://github.com/jayway/JsonPath/blob/master/LICENSE)

## Development

### Run Example

```
$ ./gradlew gem
$ embulk run -Ibuild/gemContents/lib ./example/config.yml
```


### Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Benchmark for `cache_provider` option

In some cases, `cache_provider: NOOP` improves the performance of this plugin by 3 times (https://github.com/civitaspo/embulk-filter-expand_json/pull/41/).
So we do a benchmark about `cache_provider`. In our case, `cache_provider: noop` improves the performance by 1.5 times.

|use `expand_json` filter|cache_provider|Time took|records/s|
|:---|:---|:---|:---|
|`false`|none|7.62s|1,325,459/s|
|`true`|`"LRU"`|2m9s|78,025/s|
|`true`|`"NOOP"`|1m25s|118,476/s|


You can reproduce the bench by the below way.

```
./gradlew classpath
./bench/run.sh
```

## Contributor
- @Civitaspo
- @muga
- @sakama
