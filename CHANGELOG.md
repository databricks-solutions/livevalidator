# Changelog

## v0.1.0 (2026-06-08)

First stable release. Folds all post-rc schema migrations into the base DDL and aligns version numbers across the backend, frontend, and packaging metadata.

### API

- Add `/api/{tag_id}/entities` endpoint to list entities for a tag ([`a8ca0f2`](https://github.com/databricks-field-eng/livevalidator/commit/a8ca0f2))
- Add `/api/tables/name/{name}` and `/api/queries/name/{name}` endpoints to look up entity details by name ([`1a5ef13`](https://github.com/databricks-field-eng/livevalidator/commit/1a5ef13))

### Validation engine

- Fix edge case where null `void` columns caused `except_all` runtime failures ([`57ad395`](https://github.com/databricks-field-eng/livevalidator/commit/57ad395))
- Conform types for row-by-row analysis ([`f7f30f2`](https://github.com/databricks-field-eng/livevalidator/commit/f7f30f2))

### Results view

- Multi-column sorting ([`ac7c292`](https://github.com/databricks-field-eng/livevalidator/commit/ac7c292))
- Keep the Results and Analysis tabs mounted to persist filtering state ([`03f8503`](https://github.com/databricks-field-eng/livevalidator/commit/03f8503), [`7a6c206`](https://github.com/databricks-field-eng/livevalidator/commit/7a6c206))

### Other

- Assorted bug fixes and usability tweaks.

## v0.1.0-rc (2026-05-08)

Initial release candidate. Core validation platform with web UI, scheduling, queue management, and multi-database support.
