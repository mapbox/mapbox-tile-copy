# Changelog

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
