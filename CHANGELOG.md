# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.9.3] - 2022-12-19
### Added
- [#103](https://github.com/unity-sds/unity-data-services/pull/103) return a dictionary including HREFs instead of a string REGISTERED
## [1.9.2] - 2022-11-16
### Fixed
- [#100](https://github.com/unity-sds/unity-data-services/pull/100) status=completed is only for granules, not for collections
## [1.9.1] - 2022-11-15
### Added
- [#94](https://github.com/unity-sds/unity-data-services/issues/94) Added DAPA lambdas function name to parameter store for UCS API Gateway integration
### Fixed
- [#98](https://github.com/unity-sds/unity-data-services/issues/98) accept provider from ENV or optionally from user call

## [1.8.1] - 2022-09-27
### Added
- [#79](https://github.com/unity-sds/unity-data-services/pull/79) Collection Creation endpoint with DAPA format
### Changed
- [#80](https://github.com/unity-sds/unity-data-services/pull/80) level.1.a.missing.filename
- [#82](https://github.com/unity-sds/unity-data-services/pull/82) not honoring offset and limit in Collection query
- [#89](https://github.com/unity-sds/unity-data-services/pull/89) check pathParameters is None
### Fixed


## [1.7.0] - 2022-09-06
### Added
- [#62](https://github.com/unity-sds/unity-data-services/issues/66) Added OpenAPI spec for DAPA endpoints
- [#66](https://github.com/unity-sds/unity-data-services/issues/66) Added pagination links to STAC response of DAPA endpoints
- [#64](https://github.com/unity-sds/unity-data-services/issues/64) Added temporal coverage to DAPA collection endpoint
### Changed
- [#67](https://github.com/unity-sds/unity-data-services/issues/67) Updated STAC collection schema to be compatible with PySTAC library
### Fixed

## [1.6.17] - 2022-07-28
### Added
### Fixed
- l1A granule id is `<collection-id>___<collection-version>:<granule-id>` not to duplicate re-runs

## [1.6.16] - 2022-07-25
### Added
- Added: use username & password to login to cognito to get the token 
### Fixed

## [0.1.0] - 2022-04-14
### Added
- Added lambda for parsing metadata from Sounder SIPS L0 metadata files [#14](https://github.com/unity-sds/unity-data-services/issues/14)
### Fixed
- Pushed docker image to ghcr.io
