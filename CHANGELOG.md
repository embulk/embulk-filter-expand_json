0.1.0 (2016-04-27)
==================
- [Incompatible Change]: Add stop_on_invalid_record option
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/15
  - https://github.com/civitaspo/embulk-filter-expand_json/issues/14

0.0.6 (2016-03-17)
==================
- [Add] Support JSON type
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/11
- [Enhancement] Validate json_column_name
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/9

0.0.5 (2016-03-04)
==================
- [Fix] Avoid `NullPointerException` if a column specified as `json_column_name` option doesn't exist in input schema.
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/6 from @muga
- [Fix] Migrate for Embulk v0.8.x
  - https://github.com/civitaspo/embulk-filter-expand_json/pull/7
