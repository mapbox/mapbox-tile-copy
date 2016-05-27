# mapbox-tile-copy

A shortcut from local geodata files to tiles on S3

[![Build Status](https://travis-ci.org/mapbox/mapbox-tile-copy.svg?branch=master)](https://travis-ci.org/mapbox/mapbox-tile-copy)

## Configuration

You'll be writing to S3, and so you'll need to make sure that your shell environment is [configured with appropriate credentials](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html).

## Usage

```sh
$ mapbox-tile-copy <file> <s3 url template>
```

**Note:** All [style layers](https://www.mapbox.com/mapbox-gl-style-spec/#root-layers) that use a vector source must specify a `source-layer` value per the [Mapbox GL Style Spec](https://www.mapbox.com/mapbox-gl-style-spec/). For files converted to vector tiles with map-tile-copy, the `source-layer` is the same as the filename.

### Examples:

Copy tiles from an mbtiles file to a folder in `my-bucket`:
```sh
$ mapbox-tile-copy ~/data/my-tiles.mbtiles s3://my-bucket/folder/mbtiles/{z}/{x}/{y}
```

Convert a GeoJSON file into vector tiles:
```sh
$ mapbox-tile-copy ~/data/my-data.geojson s3://my-bucket/folder/geojson/{z}/{x}/{y}
```

Copy tiles from one S3 location to another via tilejson describing the source:
```sh
$ mapbox-tile-copy ~/data/online-data.tilejson s3://my-bucket/folder/tilejson/{z}/{x}/{y}
```

Perform a part of a copy operation. [Useful for parallel processing a large file](https://github.com/mapbox/tilelive.js#parallel-read-streams):
```sh
$ mapbox-tile-copy ~/data/my-tiles.mbtiles s3://my-bucket/parallel/{z}/{x}/{y} --part 2 --parts 12
```

Render image tiles from vector tiles, using custom fonts from a location on your computer:
```sh
$ MapboxTileCopyFonts=/path/to/font/dir mapbox-tile-copy ~/style.tm2z s3://my-bucket/pngs/{z}/{x}/{y}
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

Tests involve copying files to S3. You can bring your own bucket by specifying a `TestBucket` environment variable.
```sh
$ TestBucket=my-bucket npm test
```

If you don't specify a bucket, it will attempt to write to a private Mapbox bucket, and will fail if your environment is not configured with appropriate credentials.
