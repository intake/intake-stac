# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [v0.4.0] - 2021-XX-XX

- Switched from sat-stac to pystac dependency (#72)
- CI nightly test against py36 to py39 (#71)

### Added

- Tests against python3.8 (#63)

## [v0.3.0] - 2020-10-01

### Added

- Tests against python3.8 (#63)
- Intake GUI predefined holoviews plots for thumbnails and geotiffs (#58)
- New STAC Asset mimetype driver associations (#56)
- STAC>1.0 support, with additional Jupyter Notebooks in examples/ (#52)
- StacItemCollection `to_geopandas()` method (#38)
- Use GitHub Actions for CI (#37)

## [v0.2.3] - 2019-01-21

### Removed

- Dependency of scikit-image

### Fixed

- Failed doc builds due to missing satsearch and rasterio dependency

## [v0.2.2] - 2019-12-06

### Added

- Support for `satstac.ItemCollection` objects. This facilitates integration with STAC search APIs like sat-search.

## [v0.2.1] - 2019-10-31

### Fixed

- Intake entrypoint warnings

### Added

- DOC: Setup readthedocs integration
- DOC: Add basic tutorial to documentation
- Style: Black code formatting

## [v0.2.0] - 2019-10-08

### Fixed

- Added missing requirements (intake-xarray, scikit-image)
- Add manifest to fix install

### Added

- Allow stacking of assets into a single xarray
- Updated documentation including contributing doc, readme updates
- Changelog format now uses [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)

## [v0.1.0] - 2019-05-24

Initial Release

[v0.3.0]: https://github.com/intake/intake-stac/compare/0.2.3...0.3.0
[v0.2.3]: https://github.com/intake/intake-stac/compare/0.2.2...0.2.3
[v0.2.2]: https://github.com/intake/intake-stac/compare/0.2.1...0.2.2
[v0.2.1]: https://github.com/pangeo-data/intake-stac/compare/0.2.0...0.2.1
[v0.2.0]: https://github.com/pangeo-data/intake-stac/compare/0.1.0...0.2.0
[v0.1.0]: https://github.com/pangeo-data/intake-stac/tree/0.1.0
