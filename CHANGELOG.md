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
