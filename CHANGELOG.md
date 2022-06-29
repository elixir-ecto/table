# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.2](https://github.com/dashbitco/table/tree/v0.1.2) (2022-06-29)

### Added

* Support for custom metadata in `Table.Reader.init/1`, it is now advised to include `:count` if available ([#8](https://github.com/dashbitco/table/pull/8))
* Support for passing the result of `Table.Reader.init/1` as an argument in `to_rows/2` and `to_columns/2` ([#12](https://github.com/dashbitco/table/pull/12))

### Changed

* The result of `to_rows/2` to retain enumeration properties (counting/slicing) of the underlying data ([#9](https://github.com/dashbitco/table/pull/9), [#14](https://github.com/dashbitco/table/pull/14))

### Deprecated

* The `to_rows_with_info/2` and `to_columns_with_info/2` functions in favour of using `Table.Reader.init/1` directly ([#12](https://github.com/dashbitco/table/pull/12))

## [v0.1.1](https://github.com/dashbitco/table/tree/v0.1.1) (2022-05-04)

### Changed

* Improved the performance of the rows to columns conversion ([#6](https://github.com/dashbitco/table/pull/6))

## [v0.1.0](https://github.com/dashbitco/table/tree/v0.1.0) (2022-04-27)

Initial release.
