var test = require('tape');
var sinon = require('sinon');
var nock = require('nock');
var path = require('path');
var mvtf = require('@mapbox/mvt-fixtures');
var mapnik = require('mapnik');
var mapnikVT = mapnik.VectorTile; // required for spying
var tileliveCopy = require('../lib/tilelivecopy');
var tilelive = require('@mapbox/tilelive');
var migrationStream = require('../lib/migration-stream');
var AWS = require('@mapbox/mock-aws-sdk-js');
var fs = require('fs');
var mtc = require('..'); // just to get protocols registered

function setupS3Stubs() {
  return {
    get: AWS.stub('S3', 'getObject', function(params, callback) {
      return callback(null, { Body: Buffer.from('test') });
    }),
    put: AWS.stub('S3', 'putObject', function(params, callback) {
      return callback(null);
    })
  }
}

test('copy mbtiles with v1 tile logging', function(t) {
  process.env.LOG_V1_TILES = true;
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/valid.mbtiles/{z}/{x}/{y}';
  sinon.spy(tilelive, 'copy');
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 21);
    t.equal(tilelive.copy.getCall(0).args[2].type, 'list', 'uses list scheme for mbtiles');
    t.equal(tilelive.copy.getCall(0).args[2].retry, undefined, 'passes options.retry to tilelive.copy');
    t.equal(mapnik.VectorTile.info(s3.put.args[0][0].Body).layers[0].version, 2, 'vector tile is version 2');
    
    var path = require('os').tmpdir() + '/v1-stats.json';
    t.ok(fs.existsSync(path));
    fs.unlinkSync(path);

    process.env.LOG_V1_TILES = false;
    AWS.S3.restore();
    tilelive.copy.restore();
    t.end();
  });
});

test('copy invalid mbtiles with v2 invalid tile logging', function(t) {
  process.env.LOG_INVALID_VT = true;
  var fixture = path.resolve(__dirname, 'fixtures', 'v2-throw.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/v2-throw.mbtiles/{z}/{x}/{y}';
  sinon.spy(tilelive, 'copy');
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err);
    t.equal(s3.put.callCount, 0);
    t.equal(tilelive.copy.getCall(0).args[2].type, 'list', 'uses list scheme for mbtiles');
    t.equal(tilelive.copy.getCall(0).args[2].retry, undefined, 'passes options.retry to tilelive.copy');
    
    var path = require('os').tmpdir() + '/vt-invalid.json';
    t.ok(fs.existsSync(path));
    fs.unlinkSync(path);

    process.env.LOG_INVALID_VT = false;
    AWS.S3.restore();
    tilelive.copy.restore();
    t.end();
  });
});

test('copy mbtiles without v1 tile logging', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/valid.mbtiles/{z}/{x}/{y}';
  sinon.spy(tilelive, 'copy');
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 21);
    t.equal(tilelive.copy.getCall(0).args[2].type, 'list', 'uses list scheme for mbtiles');
    t.equal(tilelive.copy.getCall(0).args[2].retry, undefined, 'passes options.retry to tilelive.copy');
    t.equal(mapnik.VectorTile.info(s3.put.args[0][0].Body).layers[0].version, 2, 'vector tile is version 2');
    
    AWS.S3.restore();
    tilelive.copy.restore();
    t.end();
  });
});

test('copy invalid mbtiles with bypassValidation option', function(t) {
  process.env.LOG_INVALID_VT = true;
  var fixture = path.resolve(__dirname, 'fixtures', 'v2-throw.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/v2-throw.mbtiles/{z}/{x}/{y}';
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, { bypassValidation: true }, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 341);
    AWS.S3.restore();
    t.end();
  });
});

test('fails with invalid ZXY from mbtiles in v1 tiles', function(t) {
  process.env.LOG_INVALID_VT = true;
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid-zxy.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/invalid-zxy.mbtiles/{z}/{x}/{y}';
  setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err);
    t.equal(err.code, 'EINVALID', 'expected code');
    t.equal(err.message, 'Tile 0/99/99 is an invalid ZXY range.', 'expected error message');
    AWS.S3.restore();
    t.end();
  });
});

test('copy retry', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/retry/{z}/{x}/{y}';
  sinon.spy(tilelive, 'copy');
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, { retry: 5 }, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 21);
    t.equal(tilelive.copy.getCall(0).args[2].type, 'list', 'uses list scheme for mbtiles');
    t.equal(tilelive.copy.getCall(0).args[2].retry, 5, 'passes options.retry to tilelive.copy');
    tilelive.copy.restore();
    AWS.S3.restore();
    t.end();
  });
});

test('copy v2 mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid-v2.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/valid-v2.mbtiles/{z}/{x}/{y}';
  sinon.spy(tilelive, 'copy');
  sinon.spy(migrationStream, 'migrate');
  sinon.spy(mapnikVT, 'info');
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 21);
    t.equal(tilelive.copy.getCall(0).args[2].type, 'list', 'uses list scheme for mbtiles');
    t.equal(tilelive.copy.getCall(0).args[2].retry, undefined, 'passes options.retry to tilelive.copy');
    t.equal(mapnikVT.info.callCount, 21, 'called mapnik info as many times as there are tiles (should only be once per v2 tile)');
    t.equal(migrationStream.migrate.notCalled, true, 'doesn\t migrate a v2 mbtiles file');
    t.equal(mapnik.VectorTile.info(s3.put.args[0][0].Body).layers[0].version, 2, 'vector tile is version 2');

    tilelive.copy.restore();
    mapnikVT.info.restore();
    migrationStream.migrate.restore();
    AWS.S3.restore();
    t.end();
  });
});

test('copy omnivore', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var src = 'omnivore://' + fixture;
  var dst = 's3://test-bucket/valid.geojson/{z}/{x}/{y}';
  var s3 = setupS3Stubs();
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, { maxzoom: 5 }, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 35);
    t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for geojson');

    AWS.S3.restore();
    tilelive.copy.restore();
    t.end();
  });
});

test('copy omnivore stats', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var src = 'omnivore://' + fixture;
  var dst = 's3://test-bucket/valid.geojson/{z}/{x}/{y}';
  setupS3Stubs();
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, { maxzoom: 5, stats: true }, function(err, stats) {
    t.ifError(err, 'copied');
    t.ok(stats, 'has stats');
    t.equal(stats.valid.geometryTypes.Polygon, 452, 'Counts polygons');

    AWS.S3.restore();
    tilelive.copy.restore();
    t.end();
  });
});

test('copy tilejson (mocks the GET request from the tilelive-tilejson module)', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.tilejson');
  var src = 'tilejson://' + fixture;
  var dst = 's3://test-bucket/valid.tilejson/{z}/{x}/{y}';
  
  var s3 = setupS3Stubs();
  sinon.spy(tilelive, 'copy');
  var requestCount = 0; 
  nock('http://test-bucket.s3.amazonaws.com')
    .get(function() {
      requestCount++;
      return true;
    }).reply(200, mvtf.get('043').buffer)
    .persist();

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied');
    t.equal(requestCount, 36 * 2, '34 GET requests to tilejson s3 location, doubled because tilelive-s3 makes a get request as well');
    t.equal(s3.put.callCount, 36, '34 tiles put to s3');
    t.equal(mapnik.VectorTile.info(s3.put.args[0][0].Body).layers[0].version, 2, 'vector tile is version 2');
    t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for tilejson');

    AWS.S3.restore();
    tilelive.copy.restore();
    nock.cleanAll();
    t.end();
  });
});

test('copy tm2z', function(t) {
  process.env.MapboxAPIMaps = 'https://api.mapbox.com';
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.tm2z');
  var src = 'tm2z://' + fixture;
  var dst = 's3://test-bucket/valid.tm2z/{z}/{x}/{y}';
  var s3 = setupS3Stubs();
  sinon.spy(tilelive, 'copy');

  tileliveCopy(src, dst, { maxzoom: 3 }, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 85, 'expected number of tiles');
    t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for tm2z');

    AWS.S3.restore();
    tilelive.copy.restore();
    t.end();
  });
});

test('copy in parallel', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/valid.mbtiles/{z}/{x}/{y}';
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, { job: { total: 10, num: 2 } }, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 4);
    AWS.S3.restore();
    t.end();
  });
});

test('copy invalid source', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.tilejson');
  var src = 'tilejson://' + fixture;
  var dst = 's3://test-bucket/invalid.tilejson/{z}/{x}/{y}';
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALID', 'marked invalid when cannot load source');
    t.equal(s3.put.callCount, 0);
    AWS.S3.restore();
    t.end();
  });
});

test('copy corrupt mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.corrupt.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/invalid.mbtiles/{z}/{x}/{y}';
  var s3 = setupS3Stubs();
  
  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'SQLITE_CORRUPT', 'pass-through errors encountered during copy');
    t.equal(s3.put.callCount, 0);
    AWS.S3.restore();
    t.end();
  });
});

test('passes through invalid tile in mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.tile-with-no-geometry.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/invalid.tile-with-no-geometry.mbtiles/{z}/{x}/{y}';
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err, stats) {
    t.ifError(err, 'passes through invalid.tile-with-no-geometry.mbtiles');
    t.equal(s3.put.callCount, 1);
    AWS.S3.restore();
    t.end();
  });
});

test('copy null-tile mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.null-tile.mbtiles');
  var src = 'mbtiles://' + fixture;
  var dst = 's3://test-bucket/invalid.mbtiles/{z}/{x}/{y}';
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALIDTILE', 'pass-through errors encountered during copy');
    t.equal(s3.put.callCount, 0);
    AWS.S3.restore();
    t.end();
  });
});

test('copy coordinates exceed spherical mercator', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.coords-out-of-range.geojson');
  var src = 'omnivore://' + fixture;
  var dst = 's3://test-bucket/invalid.geojson/{z}/{x}/{y}';
  sinon.spy(tilelive, 'copy');
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err) {
    t.ok(err, 'expect an error for out of bounds coordinates');
    t.ok(err.message.indexOf('Coordinates beyond web mercator range') > -1);
    t.equal(err.code, 'EINVALID', 'error code encountered');
    t.equal(s3.put.callCount, 0);
    AWS.S3.restore();
    tilelive.copy.restore();
    t.end();
  });
});

test('successfully copy a bigtiff', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.bigtiff.tif');
  var src = 'omnivore://' + fixture;
  var dst = 's3://test-bucket/valid.bigtiff/{z}/{x}/{y}';
  sinon.spy(tilelive, 'copy');
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied tiles');
    t.equal(s3.put.callCount, 126);
    t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for tifs');
    tilelive.copy.restore();
    AWS.S3.restore();
    t.end();
  });
});

test('copy omnivore to Frankfurt', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var src = 'omnivore://' + fixture;
  var dst = 's3://test-bucket/valid.geojson/{z}/{x}/{y}?region=eu-central-1';
  sinon.spy(tilelive, 'copy');
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, { maxzoom: 5 }, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 35);
    t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for tifs');
    t.equal(tilelive.copy.getCall(0).args[1].client.config.region, 'eu-central-1', 'uses eu-central-1 region');
    tilelive.copy.restore();
    AWS.S3.restore();
    t.end();
  });
});

test('copy omnivore to s3 encrypted with AWS KMS', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var src = 'omnivore://' + fixture;
  var dst = 's3://test-bucket/valid.geojson/{z}/{x}/{y}?sse=aws:kms&sseKmsId=alias/mapbox-tile-copy-test-kms';
  sinon.spy(tilelive, 'copy');
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, { maxzoom: 5 }, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 35);
    t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for geojson');
    t.equal(s3.put.args[0][0].SSEKMSKeyId, 'alias/mapbox-tile-copy-test-kms');
    t.equal(s3.put.args[0][0].ServerSideEncryption, 'aws:kms');
    t.equal(s3.put.args[0][0].ACL, 'private');
    AWS.S3.restore();
    tilelive.copy.restore();
    t.end();
  });
});

test('handles vector data reprojection', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'reprojection/data.shp');
  var src = 'omnivore://' + fixture;
  var dst = 's3://test-bucket/reprojection.shp/{z}/{x}/{y}';
  sinon.spy(tilelive, 'copy');
  var s3 = setupS3Stubs();

  tileliveCopy(src, dst, {}, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.callCount, 35);
    t.equal(tilelive.copy.getCall(0).args[2].type, 'scanline', 'uses scanline scheme for shp reprojection');
    AWS.S3.restore();
    tilelive.copy.restore();
    t.end();
  });
});
