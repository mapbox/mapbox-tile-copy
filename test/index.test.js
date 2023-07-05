'use strict';

var test = require('tape').test;
var path = require('path');
var AWS = require('@mapbox/mock-aws-sdk-js');
var copy = require('../index.js');

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

test('serialtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.serialtiles.gz');
  var dst = 's3://test-bucket/valid.serialtiles/{z}/{x}/{y}';
  var s3 = setupS3Stubs();

  copy(fixture, dst, {}, function(err) {
    t.ifError(err, 'copied');
    t.equal(AWS.S3.callCount, 1, '1 s3 client');
    t.equal(s3.get.callCount, 4, '4 tiles retrieved');
    t.equal(s3.put.callCount, 4, '4 tiles put to s3');
    t.deepEqual(s3.put.args.map(function(call) {
      return call[0].Key;
    }).sort(), [
      'valid.serialtiles/1/0/0', 
      'valid.serialtiles/1/0/1', 
      'valid.serialtiles/1/1/0', 
      'valid.serialtiles/1/1/1'
    ]);
    AWS.S3.restore();
    t.end();
  });
});

test('mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var dst = 's3://test-bucket/valid.mbtiles/{z}/{x}/{y}';
  var s3 = setupS3Stubs();

  copy(fixture, dst, {}, function(err) {
    t.ifError(err, 'copied');
    t.equal(AWS.S3.callCount, 1, '1 s3 client');
    t.equal(s3.get.callCount, 21, '21 tiles retrieved');
    t.equal(s3.put.callCount, 21, '21 tiles put to s3');
    t.deepEqual(s3.put.args.map(function(call) {
      return call[0].Key;
    }).sort(), [
      'valid.mbtiles/0/0/0',
      'valid.mbtiles/1/0/0',
      'valid.mbtiles/1/0/1',
      'valid.mbtiles/1/1/0',
      'valid.mbtiles/1/1/1',
      'valid.mbtiles/2/0/0',
      'valid.mbtiles/2/0/1',
      'valid.mbtiles/2/0/2',
      'valid.mbtiles/2/0/3',
      'valid.mbtiles/2/1/0',
      'valid.mbtiles/2/1/1',
      'valid.mbtiles/2/1/2',
      'valid.mbtiles/2/1/3',
      'valid.mbtiles/2/2/0',
      'valid.mbtiles/2/2/1',
      'valid.mbtiles/2/2/2',
      'valid.mbtiles/2/2/3',
      'valid.mbtiles/2/3/0',
      'valid.mbtiles/2/3/1',
      'valid.mbtiles/2/3/2',
      'valid.mbtiles/2/3/3'
    ]);
    AWS.S3.restore();
    t.end();
  });
});

test('corrupt mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.corrupt.mbtiles');
  var dst = 's3://test-bucket/invalid.corrupt.mbtiles/{z}/{x}/{y}';
  var s3 = setupS3Stubs();

  copy(fixture, dst, {}, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALID', 'marked invalid');
    t.equal(s3.put.callCount, 0, '0 tiles put to s3');
    AWS.S3.restore();
    t.end();
  });
});

test('null mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.null-tile.mbtiles');
  var dst = 's3://test-bucket/invalid.null-tile.mbtiles/{z}/{x}/{y}';
  var s3 = setupS3Stubs();

  copy(fixture, dst, {}, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALID', 'marked invalid');
    t.equal(s3.put.callCount, 0, '0 tiles put to s3');
    AWS.S3.restore();
    t.end();
  });
});

test('fails with missing {z}/{x}/{y} template', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var dst = 's3://test-bucket/valid.mbtiles/{z}/{x}/{y}';
  var dst = dst.slice(0, dst.indexOf('{z}'));
  var s3 = setupS3Stubs();

  copy(fixture, dst, {}, function(err) {
    t.ok(err);
    t.equal(err.message, 'Destination URL does not include a {z}/{x}/{y} template.');
    t.equal(s3.put.callCount, 0, '0 tiles put to s3');
    AWS.S3.restore();
    t.end();
  });
});
