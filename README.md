# Expand Json filter plugin for Embulk

expand columns having json into multiple columns

## Overview

* **Plugin type**: filter

## Configuration

- **json_column_name**: a column name having json to be expanded (string, required)
- **expanded_columns**: columns expanded into multiple columns (array of hash, required)
  - **name**: name of the column. you can define [JsonPath](http://goessner.net/articles/JsonPath/) style.
  - **type**: type of the column (see below)
  - **format**: format of the timestamp if type is timestamp

## Example

```yaml
filters:
  - type: expand_json
    json_column_name: json_payload
    expanded_columns:
      - {name: "$.phone_numbers", type: string}
      - {name: "$.app_id", type: long}
      - {name: "$.point", type: double}
      - {name: "$.created_at", type: timestamp, format: "%Y-%m-%d"}
      - {name: "$.profile.anniversary.et", type: string}
      - {name: "$.profile.anniversary.voluptatem", type: string}
      - {name: "$.profile.like_words[1]", type: string}
      - {name: "$.profile.like_words[2]", type: string}
      - {name: "$.profile.like_words[0]", type: string}
```

## Note
- If the value evaluated by JsonPath is Array or Hash, the value is written as JSON.

## Dependencies
- https://github.com/jayway/JsonPath
  - use to evaluate [JsonPath](http://goessner.net/articles/JsonPath/)
  - [Apache License Version 2.0](https://github.com/jayway/JsonPath/blob/master/LICENSE)


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
