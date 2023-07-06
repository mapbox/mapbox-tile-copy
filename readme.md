# mapbox-tile-copy

A shortcut from local geodata files to tiles on S3 or to the local filesystem.

[![Build Status](https://travis-ci.com/mapbox/mapbox-tile-copy.svg?branch=master)](https://travis-ci.com/mapbox/mapbox-tile-copy)

## Installation

```sh
$ npm install -g @mapbox/mapbox-tile-copy
```

## Configuration

If writing to S3 you'll need to make sure that your shell environment is [configured with appropriate credentials](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html).

## Usage

```sh
$ mapbox-tile-copy <file> <s3:// url template or file:// path>
```

Your s3 url template must include a `{z}/{x}/{y}` scheme for writing and distributing tiles. File extensions are not required.

#### Examples:

Copy tiles from an mbtiles file to a folder in `my-bucket`:
```sh
$ mapbox-tile-copy ~/data/my-tiles.mbtiles s3://my-bucket/folder/mbtiles/{z}/{x}/{y}
```

Copy tiles from an mbtiles file to a relative folder in the current working directory named 'tiles':
```sh
$ mapbox-tile-copy ~/data/my-tiles.mbtiles file://./tiles
```

Convert a GeoJSON file into vector tiles:
```sh
$ mapbox-tile-copy ~/data/my-data.geojson s3://my-bucket/folder/geojson/{z}/{x}/{y}
```

Copy tiles from one S3 location to another via tilejson describing the source:
```sh
$ mapbox-tile-copy ~/data/online-data.tilejson s3://my-bucket/folder/tilejson/{z}/{x}/{y}
```

Render image tiles from vector tiles, using custom fonts from a location on your computer:
```sh
$ MapboxTileCopyFonts=/path/to/font/dir mapbox-tile-copy ~/style.tm2z s3://my-bucket/pngs/{z}/{x}/{y}
```

Perform a part of a copy operation. [Useful for parallel processing a large file](https://github.com/mapbox/tilelive.js#parallel-read-streams):
```sh
$ mapbox-tile-copy ~/data/my-tiles.mbtiles s3://my-bucket/parallel/{z}/{x}/{y} --part 2 --parts 12
```

The `--part` operation is explicitly _zero-indexed_ because this gives tilelive's stream processors a predictable way to segment tiles per part. For example, the following will distribute all tiles among a single part. So all tiles will be rendered by this single part:
```sh
$ mapbox-tile-copy ~/data/my-tiles.mbtiles s3://my-bucket/parallel/{z}/{x}/{y} --part 0 --parts 1
```

The following example will distribute tiles to the second part out of 4 total parts:
```sh
$ mapbox-tile-copy ~/data/my-tiles.mbtiles s3://my-bucket/parallel/{z}/{x}/{y} --part 1 --parts 4
```

You can add extra parameters supported by [tilelive-s3](https://github.com/mapbox/tilelive-s3) onto the end of the S3 URL. For instance, to extend the default HTTP timeout of 2000ms:
```sh
$ mapbox-tile-copy ~/data/my-tiles.mbtiles s3://my-bucket//{z}/{x}/{y}?timeout=10000
```

Collect tile size statistics and dump to your local tmp dir named `/tmp/<tmpdirpath>/tilelive-bridge-stats.json`
```sh
$ BRIDGE_LOG_MAX_VTILE_BYTES_COMPRESSED=1 mapbox-tile-copy ~/data/my-tiles.mbtiles s3://my-bucket/folder/mbtiles/{z}/{x}/{y}
```

## Supported file types

- .mbtiles
- .tilejson
- .tm2z
- .kml
- .geojson
- .gpx
- .csv
- .shp
- .tif
- .vrt
- serialtiles

## Running tests

```sh
$ npm test
```