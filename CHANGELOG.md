# Changelog

## 6.7.0

- Adds x out of range error handler. See #113
- Adds file writer for invalid VT stats object & vtvalidate library for additional VT validation. See #113 & mapbox/unpacker#1458 (comment)

## 6.6.0

- Remove duplicate mapnik.VectorTile.info call in migration stream for v2 tiles [#102](https://github.com/mapbox/mapbox-tile-copy/pull/102)
- Catch tile size errors from tilelive-bridge and exit with `EINVALID`

## 6.5.0

- Add support for s3 KMS encryption [#108](https://github.com/mapbox/mapbox-tile-copy/pull/108) and update test user credentials [#109](https://github.com/mapbox/mapbox-tile-copy/pull/109)
- Catch reprojection errors from mapnik [#110](https://github.com/mapbox/mapbox-tile-copy/pull/110)

## 6.4.0

- Add V1 tile tracker.

## N/A

- Update tests per https://github.com/mapbox/mapnik-omnivore/pull/172

## 6.3.0

- Upgrade tilelive-omnivore to 3.4.0

## 6.2.1

- Upgrade tilelive, tilejson, tiletype, mbtiles, and s3urls deps to their new @mapbox namespace

## 6.2.0

- Upgraded to use mapnik 3.6.0, along with relevant deps:
  - tilelive-omnivore@3.3.0
  - tilelive-vector@3.10.0

## 6.1.0

- Upgrade to tilelive-omnivore@3.2.0

## 6.0.2

- Add namespaced tilelive-s3@6.5.1

## 6.0.1

- Add namespaced omnivores and add z23 bump

## 6.0.0

- Change default tiling scheme to `scanline` for the omnivore protocol, which resolves an issue where z0 tiles were being simplified away and no tiles were counted further down the pyramid.

## 5.1.1

- Add error handling for missing `{z}/{x}/{y}` template in the destination URL.
- test to make sure that zero-indexing is happening correctly (all tiles copied to a single part)

## 5.1.0

- Upgrade mapnik-omnivore#8.1.0, tilelive-omnivore@3.1.0, mapbox-file-sniff@0.5.2, mbtiles@0.9.0
- Add tests for Node 6
- Update tests using aws-sdk region param per https://github.com/mapbox/mapbox-tile-copy/pull/81

## 5.0.0

- Output tilesets will never have a max zoom level less than 6

## 4.8.1

- Bump to tilelive-s3@6.4.1

## 4.8.0

- Can specify the region for the destination bucket by providing a `region` query param

## 4.7.1

- Add explicit error to the `copy` operation for coordinates out of bounds.

## 4.7.0

- For tilejson copy operations, migrate each tile to vt2

## 4.6.2

- Validate mbtiles uploads after migrating the tiles to v2

## 4.6.1

- Only [log the error message](https://github.com/mapbox/mapbox-tile-copy/pull/70) from `bin/mapbox-tile-copy.js` instead of the full stack trace

## 4.6.0

- Bundle support: Ability to handle multiple files for a single tileset

## 4.5.0

- Upgraded to tilelive-omnivore@2.4.0.

## 4.4.0

- Allow layerName to be set on source uris

## 4.3.2

- Upgraded to latest node-mapnik v3.5.x release: v3.5.9

## 4.3.1

- Correctly extracts tile and layer parsing errors

## 4.3.0

- Add a transform for mbtiles copy operations to migrate V1 vector tiles to V2

## 4.2.2

- Upgraded to tilelive-s3@6.2.0 - [support for `$AWS_S3_ENDPOINT` variable](https://github.com/mapbox/tilelive-s3/pull/79)

## 4.1.2

- [Add filesize validation](https://github.com/mapbox/mapbox-upload-limits/pull/5) for `max_filesize` for `mbtiles`, `tm2z`, and `serialtiles`
- [Increased filesize for GeoJSON and CSV to 1GB](https://github.com/mapbox/mapbox-upload-limits/pull/7)
- [Fixed csv-detection bug](https://github.com/mapbox/mapbox-file-sniff/pull/37/files)

## 4.1.1

- Upgraded to mapbox-upload-validate@3.1.0

## 4.1.0

- Upgraded to tilelive-omnivore@2.1.0.
