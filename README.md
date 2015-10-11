# Expand Json filter plugin for Embulk

TODO: Write short description here and build.gradle file.

## Overview

* **Plugin type**: filter

## Configuration

- **option1**: description (integer, required)
- **option2**: description (string, default: `"myvalue"`)
- **option3**: description (string, default: `null`)

## Example

```yaml
filters:
  - type: expand_json
    option1: example1
    option2: example2
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
