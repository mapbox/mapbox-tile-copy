var test = require('tape');
var path = require('path');
var mapnik = require('mapnik');
var mapnikVT = mapnik.VectorTile; // required for spying
var crypto = require('crypto');
var tileliveCopy = require('../lib/tilelivecopy');
var tilelive = require('@mapbox/tilelive');
var migrationStream = require('../lib/migration-stream');
var AWS = require('aws-sdk');
var s3urls = require('@mapbox/s3urls');
var os = require('os');
var fs = require('fs');
var mtc = require('..'); // just to get protocols registered
var sinon = require('sinon');
var TileStatStream = require('tile-stat-stream');
var mvtf = require('@mapbox/mvt-fixtures');

process.env.MapboxAPIMaps = 'https://api.tiles.mapbox.com';

var bucket = process.env.TestBucket || 'tilestream-tilesets-development';
var runid = crypto.randomBytes(16).toString('hex');

console.log('---> mapbox-tile-copy copy.tilelive %s', runid);

function dsturi(name) {
  return [
    's3:/',
    bucket,
    'test/mapbox-tile-copy',
    runid,
    name,
    '{z}/{x}/{y}'
  ].join('/');
}

function tileCount(dst, callback) {
  var count = 0;
  var region = require('url').parse(dst, true).query.region;
  var params = s3urls.fromUrl(dst.replace('{z}/{x}/{y}', ''));
  params.Prefix = params.Key;
  delete params.Key;

  var s3 = new AWS.S3({ region: region });

  function list(marker) {
    if (marker) params.Marker = marker;

    s3.listObjects(params, function(err, data) {
      if (err) return callback(err);
      count += data.Contents.length;
      if (data.IsTruncated) return list(data.Contents.pop().Key);
      callback(null, count);
    });
  }

  list();
}

function tileVersion(dst, z, x, y, callback) {
  var s3 = new AWS.S3();
  var count = 0;

  var params = s3urls.fromUrl(dst.replace('{z}/{x}/{y}', z + '/' + x + '/' + y));

  s3.getObject(params, function(err, data) {
    if (err) return callback(err);

    var info = mapnik.VectorTile.info(data.Body);
    var version = info.layers[0].version;
    return callback(null, version);
  });
}

test('copy mbtiles with v1 tile logging', function(t) {
  process.env.LOG_V1_TILES = true;
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('valid.mbtiles');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.equal(tilelive.copy.getCall(0).args[2].type, 'list', 'uses list scheme for mbtiles');
      t.equal(tilelive.copy.getCall(0).args[2].retry, undefined, 'passes options.retry to tilelive.copy');
      tilelive.copy.restore();

      tileVersion(dst, 0, 0, 0, function(err, version) {
        var path = require('os').tmpDir() + '/v1-stats.json';
        t.ok(fs.existsSync(path));
        process.env.LOG_V1_TILES = false;
        fs.unlinkSync(path);
        t.end();
      });
    });
  });
});

test('copy invalid mbtiles with v2 invalid tile logging', function(t) {
  process.env.LOG_INVALID_VT = true;
  var fixture = path.resolve(__dirname, 'fixtures', 'v2-throw.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('v2-throw.mbtiles');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, {}, function(err) {
    tileCount(dst, function(err, count) {
      t.equal(tilelive.copy.getCall(0).args[2].type, 'list', 'uses list scheme for mbtiles');
      t.equal(tilelive.copy.getCall(0).args[2].retry, undefined, 'passes options.retry to tilelive.copy');
      tilelive.copy.restore();

      tileVersion(dst, 0, 0, 0, function(err, version) {
        var path = require('os').tmpDir() + '/vt-invalid.json';
        t.ok(fs.existsSync(path));
        process.env.LOG_V1_TILES = false;
        fs.unlinkSync(path);
        t.end();
      });
    });
  });
});


test('copy mbtiles without v1 tile logging', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('valid.mbtiles');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 21, 'expected number of tiles');
      t.equal(tilelive.copy.getCall(0).args[2].type, 'list', 'uses list scheme for mbtiles');
      t.equal(tilelive.copy.getCall(0).args[2].retry, undefined, 'passes options.retry to tilelive.copy');
      tilelive.copy.restore();

      tileVersion(dst, 0, 0, 0, function(err, version) {
        t.ifError(err, 'got tile info');
        t.equal(version, 2, 'tile is v2');
        t.end();
      });
    });
  });
});

test('copy invalid mbtiles with bypassValidation option', function(t) {
  process.env.LOG_INVALID_VT = true;
  var fixture = path.resolve(__dirname, 'fixtures', 'v2-throw.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('v2-throw.mbtiles');

  tileliveCopy(src, dst, { bypassValidation: true }, function(err) {
    t.ifError(err);
    tileCount(dst, function(err, count) {
      t.ifError(err);
      t.equal(count, 341, 'expected number of tiles');
      t.end();
    });
  });
});

test.only('fails with invalid ZXY from mbtiles in v1 tiles', function(t) {
  process.env.LOG_INVALID_VT = true;
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid-zxy.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('invalid-zxy.mbtiles');

  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err);
    t.equal(err.code, 'EINVALID', 'expected code');
    t.equal(err.message, 'Tile 0/99/99 is an invalid ZXY range.', 'expected error message');
    t.end();
  });
});

test('copy retry', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('retry');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, {retry:5}, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 21, 'expected number of tiles');
      t.equal(tilelive.copy.getCall(0).args[2].type, 'list', 'uses list scheme for mbtiles');
      t.equal(tilelive.copy.getCall(0).args[2].retry, 5, 'passes options.retry to tilelive.copy');
      tilelive.copy.restore();
      t.end();
    });
  });
});

test('copy v2 mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid-v2.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('valid-v2.mbtiles');
  sinon.spy(tilelive, 'copy');
  sinon.spy(migrationStream, 'migrate');
  sinon.spy(mapnikVT, 'info');

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 21, 'expected number of tiles');
      t.equal(tilelive.copy.getCall(0).args[2].type, 'list', 'uses list scheme for mbtiles');
      t.equal(tilelive.copy.getCall(0).args[2].retry, undefined, 'passes options.retry to tilelive.copy');
      tilelive.copy.restore();

      t.equal(mapnikVT.info.callCount, count, 'called mapnik info as many times as there are tiles (should only be once per v2 tile)');
      mapnikVT.info.restore();

      t.equal(migrationStream.migrate.notCalled, true, 'doesn\t migrate a v2 mbtiles file');
      migrationStream.migrate.restore();

      tileVersion(dst, 0, 0, 0, function(err, version) {
        t.ifError(err, 'got tile info');
        t.equal(version, 2, 'tile is v2');
        t.end();
      });
    });
  });
});

test('copy omnivore', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var src = 'omnivore://' + fixture;
  var dst = dsturi('valid.geojson');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, { maxzoom: 5 }, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 35, 'expected number of tiles');
      t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for geojson');
      tilelive.copy.restore();
      t.end();
    });
  });
});

test('copy omnivore stats', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var src = 'omnivore://' + fixture;
  var dst = dsturi('valid.geojson');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, { maxzoom: 5, stats: true }, function(err, stats) {
    t.ifError(err, 'copied');
    t.ok(stats, 'has stats');
    t.equal(stats.valid.geometryTypes.Polygon, 452, 'Counts polygons');
    tilelive.copy.restore();
    t.end();
  });
});

test('copy tilejson', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.tilejson');
  var tmp = path.join(os.tmpdir(), crypto.randomBytes(16).toString('hex'));

  var onlineTiles = dsturi('online') + '?acl=public-read';
  tilelive.copy('omnivore://' + path.resolve(__dirname, 'fixtures', 'valid.geojson'), onlineTiles, {
    maxzoom: 5,
    type: 'pyramid',
    minzoom: 4,
    bounds: [
      -124.76214599609376,
      24.54521942138596,
      -66.95780181884764,
      49.3717422485226
    ],
    close: true
  }, function(err) {
    t.ifError(err);
    sinon.spy(tilelive, 'copy');
    fs.readFile(fixture, 'utf8', function(err, data) {
      t.ifError(err, 'fixture load');
      data = JSON.parse(data);
      data.tiles = [ s3urls.convert(onlineTiles, 'bucket-in-host') ];
      fs.writeFile(tmp, JSON.stringify(data), runCopy);
    });
  });

  function runCopy() {
    var src = 'tilejson://' + tmp;
    var dst = dsturi('valid.tilejson');
    tileliveCopy(src, dst, {}, function(err) {
      t.ifError(err, 'copied');
      tileCount(dst, function(err, count) {
        t.ifError(err, 'counted tiles');
        t.equal(count, 27, 'expected number of tiles');

        tileVersion(dst, 4, 2, 5, function(err, version) {
          t.ifError(err, 'got tile info');
          t.equal(version, 2, 'tile is v2');
          t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for tilejson');
          tilelive.copy.restore();
          t.end();
        });
      });
    });
  }
});

test('copy tm2z', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.tm2z');
  var src = 'tm2z://' + fixture;
  var dst = dsturi('valid.tm2z');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, { maxzoom: 3 }, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 85, 'expected number of tiles');
      t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for tm2z');
      tilelive.copy.restore();
      t.end();
    });
  });
});

test('copy in parallel', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('parallel');
  tileliveCopy(src, dst, { job: { total: 10, num: 2 } }, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.ok(count < 21, 'did not render all tiles');
      t.end();
    });
  });
});

test('copy invalid source', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.tilejson');
  var src = 'tilejson://' + fixture;
  var dst = dsturi('invalid.tilejson');
  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALID', 'marked invalid when cannot load source');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 0, 'rendered no tiles');
      t.end();
    });
  });
});

test('copy corrupt mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.corrupt.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('invalid.mbtiles');
  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'SQLITE_CORRUPT', 'pass-through errors encountered during copy');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 0, 'did not render any tiles');
      t.end();
    });
  });
});

test('passes through invalid tile in mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.tile-with-no-geometry.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('invalid.tile-with-no-geometry.mbtiles');
  tileliveCopy(src, dst, {}, function(err, stats) {
    t.ifError(err, 'passes through invalid.tile-with-no-geometry.mbtiles');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 1, 'rendered all tiles');
      t.end();
    });
  });
});

test('copy null-tile mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.null-tile.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = dsturi('invalid.mbtiles');
  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALIDTILE', 'pass-through errors encountered during copy');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 0, 'did not render any tiles');
      t.end();
    });
  });
});

test('copy coordinates exceed spherical mercator', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.coords-out-of-range.geojson');
  var src = 'omnivore://' + fixture;
  var dst = dsturi('invalid.geojson');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err, 'expect an error for out of bounds coordinates');
    t.ok(err.message.indexOf('Coordinates beyond web mercator range') > -1);
    t.equal(err.code, 'EINVALID', 'error code encountered');
    tilelive.copy.restore();
    t.end();
  });
});

test('successfully copy a bigtiff', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.bigtiff.tif');
  var src = 'omnivore://' + fixture;
  var dst = dsturi('valid.bigtiff');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied tiles');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 126, 'rendered all tiles');
      t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for tifs');
      tilelive.copy.restore();
      t.end();
    });
  });
});

test('copy omnivore to Frankfurt', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var src = 'omnivore://' + fixture;
  var dst = [
    's3://mapbox-eu-central-1/test/mapbox-tile-copy',
    runid,
    'valid.geojson/{z}/{x}/{y}?region=eu-central-1'
  ].join('/');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, { maxzoom: 5 }, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 35, 'expected number of tiles');
      t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for geojson');
      tilelive.copy.restore();
      t.end();
    });
  });
});

test('copy omnivore to s3 encrypted with AWS KMS', function(t) {
  var kmsKeyId = 'alias/mapbox-tile-copy-test-kms';
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var src = 'omnivore://' + fixture;
  var dst = [
    's3://' + bucket + '/test/mapbox-tile-copy',
    runid,
    'kms-encrypted.geojson/{z}/{x}/{y}?sse=aws:kms&sseKmsId=' + kmsKeyId
  ].join('/');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, { maxzoom: 5 }, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 35, 'expected number of tiles');
      t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for geojson');
      tilelive.copy.restore();
      t.end();
    });
  });
});

test('handles vector data reprojection', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'reprojection/data.shp');
  var src = 'omnivore://' + fixture;
  var dst = dsturi('reprojection.shp');
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 35, 'expected number of tiles');
      t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for geojson');
      tilelive.copy.restore();
      t.end();
    });
  });
});
