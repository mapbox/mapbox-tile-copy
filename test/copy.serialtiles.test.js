'use strict';

var test = require('tape').test;
var sinon = require('sinon');
var path = require('path');
var mapnik = require('mapnik');
var AWS = require('@mapbox/mock-aws-sdk-js');
var tilelive = require('@mapbox/tilelive');
var copy = require('../lib/serialtilescopy');
var migrationStream = require('../lib/migration-stream');

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

test('serialtiles-copy: gzipped vector tiles', function(t) {
  var uri = [
    'serialtiles:',
    path.resolve(__dirname, 'fixtures', 'valid.serialtiles.gzip.vector.gz')
  ].join('//');
  var dst = 'http://test-bucket.s3.amazonaws.com/_pending/test/test.valid-gzip/{z}/{x}/{y}';

  sinon.spy(tilelive, 'createWriteStream');
  var s3 = setupS3Stubs();

  copy(uri, dst, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.args[0][0].ContentEncoding, 'gzip');
    t.equal(s3.put.args[0][0].ContentType, 'application/x-protobuf');
    t.equal(mapnik.VectorTile.info(s3.put.args[0][0].Body).layers[0].version, 2, 'vector tile is version 2');
    t.equal(tilelive.createWriteStream.getCall(0).args[1].retry, undefined, 'passes options.retry to tilelive.createWriteStream');
    tilelive.createWriteStream.restore();
    AWS.S3.restore();
    t.end();
  });
});

test('serialtiles-copy: options.retry', function(t) {
  var uri = [
    'serialtiles:',
    path.resolve(__dirname, 'fixtures', 'valid.serialtiles.gzip.vector.gz')
  ].join('//');

  var dst = 'http://test-bucket.s3.amazonaws.com/_pending/test/test.retry/{z}/{x}/{y}';

  sinon.spy(tilelive, 'createWriteStream');
  var s3 = setupS3Stubs();

  copy(uri, dst, { retry: 5 }, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.args[0][0].ContentEncoding, 'gzip');
    t.equal(s3.put.args[0][0].ContentType, 'application/x-protobuf');
    t.equal(mapnik.VectorTile.info(s3.put.args[0][0].Body).layers[0].version, 2, 'vector tile is version 2');
    t.equal(tilelive.createWriteStream.getCall(0).args[1].retry, 5, 'passes options.retry to tilelive.createWriteStream');
    tilelive.createWriteStream.restore();
    AWS.S3.restore();
    t.end();
  });
});

test('serialtiles-copy: gzipped vector tiles, v2', function(t) {
  var uri = [
    'serialtiles:',
    path.resolve(__dirname, 'fixtures', 'valid-v2.serialtiles.gzip.vector.gz')
  ].join('//');
  var dst = 'http://test-bucket.s3.amazonaws.com/_pending/test/test.valid-v2-gzip/{z}/{x}/{y}';

  sinon.spy(tilelive, 'createWriteStream');
  sinon.spy(migrationStream, 'migrate');
  var s3 = setupS3Stubs();

  copy(uri, dst, function(err) {
    t.ifError(err, 'copied');
    t.equal(s3.put.args[0][0].ContentEncoding, 'gzip');
    t.equal(s3.put.args[0][0].ContentType, 'application/x-protobuf');
    t.equal(mapnik.VectorTile.info(s3.put.args[0][0].Body).layers[0].version, 2, 'vector tile is version 2');
    t.equal(tilelive.createWriteStream.getCall(0).args[1].retry, undefined, 'passes options.retry to tilelive.createWriteStream');
    t.equal(migrationStream.migrate.notCalled, true, 'doesn\t migrate a v2 mbtiles file');
    tilelive.createWriteStream.restore();
    migrationStream.migrate.restore();
    AWS.S3.restore();
    t.end();
  });
});

test('serialtiles-copy: parallel processing', function(t) {
  var uri = [
    'serialtiles:',
    path.resolve(__dirname, 'fixtures', 'valid.serialtiles.gzip.vector.gz')
  ].join('//');
  var dst = 'http://test-bucket.s3.amazonaws.com/_pending/test/test.valid-parallel/{z}/{x}/{y}';

  var s3 = setupS3Stubs();

  copy(uri, dst, { job: { num: 0, total: 10 }}, function(err) {
    t.ifError(err, 'copied');
    t.ok(s3.put.callCount < 21, 'should not render the entire dataset');
    AWS.S3.restore();
    t.end();
  });
});

test('serialtiles-copy: stats', function(t) {
  var uri = [
    'serialtiles:',
    path.resolve(__dirname, 'fixtures', 'valid.serialtiles.gzip.vector.gz')
  ].join('//');
  var dst = 'http://test-bucket.s3.amazonaws.com/_pending/test/test.valid-parallel/{z}/{x}/{y}';
  
  setupS3Stubs();

  copy(uri, dst, { stats: true, job: { num: 0, total: 10 } }, function(err, stats) {
    t.ifError(err, 'no error');
    t.equal(stats.world_merc.count, 223);
    AWS.S3.restore();
    t.end();
  });
});

test('serialtiles-copy: tiles too big', function(t) {
  var uri = [
    'serialtiles:',
    path.resolve(__dirname, 'fixtures', 'valid.serialtiles.gzip.vector.gz')
  ].join('//');
  var dst = 'http://test-bucket.s3.amazonaws.com/_pending/test/test.invalid-tilesize/{z}/{x}/{y}';
  setupS3Stubs();

  copy(uri, dst, { limits: { max_tilesize: 10 } }, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALID', 'expected error code');
    t.equal(err.message, 'Tile exceeds maximum size of 0k at z 0. Reduce the detail of data at this zoom level or omit it by adjusting your minzoom.', 'expected error message');
    AWS.S3.restore();
    t.end();
  });
});

test('serialtiles-copy: vector tile invalid', function(t) {
  var uri = [
    'serialtiles:',
    path.resolve(__dirname, 'fixtures', 'invalid.serialtiles.gzipped.gz')
  ].join('//');
  var dst = 'http://test-bucket.s3.amazonaws.com/_pending/test/test.invalid-vector-tile/{z}/{x}/{y}';
  setupS3Stubs();

  copy(uri, dst, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALID', 'expected error code');
    t.equal(err.message, 'Buffer is not encoded as a valid PBF', 'expected error message');
    t.ok(err.stack, 'error has stacktrace');
    AWS.S3.restore();
    t.end();
  });
});
