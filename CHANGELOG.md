0.5.4 (2022-03-02)
==================

- [Enhancement] setCacheProviderOrIgnore in transaction
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/58

0.5.3 (2021-06-29)
==================
- [Enhancement] Catch up with Embulk v0.10 API/SPI
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/55
- [Enhancement] Rename the default branch: master -> main
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/56

0.5.2 (2020-08-31)
==================
- [Enhancement] Update embulk-util-timestamp to 0.2.1
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/5

0.5.1 (2020-08-17)
==================
- [Enhancement] Use embulk-util-config:0.1.1 instead of ConfigSource#loadConfig and TaskSource#loadTask
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/49

0.5.0 (2020-08-13)
==================
- [Enhancement] Run CIs on pull requests from forked repositories.
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/50
- [Enhancement] Catch up with Embulk v0.10.6
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/48
  - This change will still work correctly with Embulk v0.9.23.

0.4.0 (2020-04-24)
==================
- [Enhancement] Build with the "org.embulk.embulk-plugins" Gradle plugin
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/45
- [Enhancement] Use Github Actions instead of Travis CI.
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/46

0.3.0 (2019-05-02)
==================
- [Enhancement] Introduce `cache_provider` option.
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/41

0.2.2 (2017-09-14)
==================
- [Enhancement] Use TimestampParser's constructor without JRuby ScriptingContainer
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/37
- [Enhancement] embulk migrate .
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/36

0.2.1 (2017-09-12)
==================
- [Enhancement] Support type conversion from floating point numbers to integers
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/33

0.2.0 (2017-07-14)
==================
- [Incompatible Change]: Remove `time_zone` option, use `default_timezone` instead and column-based timezone.
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/27
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/28


0.1.3 (2016-07-27)
==================
- [Enhancement] Improve Exception handling
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/23

0.1.2 (2016-05-31)
==================
- [Enhancement] Add a validation to check column duplication
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/21

0.1.1 (2016-05-02)
==================
- [New Feature] Add keep_expanding_json_column option
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/19
- [Fix] ix retrieving unchanged json columns
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/17

0.1.0 (2016-04-27)
==================
- [Incompatible Change / New Feature]: Add stop_on_invalid_record option
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/15
  - https://github.com/civitaspo/embulk-filter-expand_json/issues/14

0.0.6 (2016-03-17)
==================
- [New Feature] Support JSON type
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/11
- [Enhancement] Validate json_column_name
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/9

0.0.5 (2016-03-04)
==================
- [Fix] Avoid `NullPointerException` if a column specified as `json_column_name` option doesn't exist in input schema.
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/6 from @muga
- [Fix] Migrate for Embulk v0.8.x
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/7
