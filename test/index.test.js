var test = require('tape').test;
var path = require('path');
var crypto = require('crypto');
var copy = require('../index.js');
var AWS = require('aws-sdk');
var s3urls = require('s3urls');

process.env.MapboxAPIMaps = 'https://api.tiles.mapbox.com';

var bucket = process.env.TestBucket || 'tilestream-tilesets-development';
var runid = crypto.randomBytes(16).toString('hex');

console.log('---> mapbox-tile-copy index %s', runid);

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
  var s3 = new AWS.S3();
  var count = 0;

  params = s3urls.fromUrl(dst.replace('{z}/{x}/{y}', ''));
  params.Prefix = params.Key;
  delete params.Key;

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

test('serialtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.serialtiles.gz');
  var dst = dsturi('valid.serialtiles');
  copy(fixture, dst, {}, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 4, 'expected number of tiles');
      t.end();
    });
  });
});

test('mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var dst = dsturi('valid.mbtiles');
  copy(fixture, dst, {}, function(err) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 21, 'expected number of tiles');
      t.end();
    });
  });
});

test('corrupt mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.corrupt.mbtiles');
  var dst = dsturi('invalid.corrupt.mbtiles');
  copy(fixture, dst, {}, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALID', 'marked invalid');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 0, 'rendered no tiles');
      t.end();
    });
  });
});

test('null mbtiles', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.null-tile.mbtiles');
  var dst = dsturi('invalid.null-tile.mbtiles');
  copy(fixture, dst, {}, function(err) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALID', 'marked invalid');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 0, 'rendered no tiles');
      t.end();
    });
  });
});

test('fails with missing {z}/{x}/{y} template', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
  var dst = dsturi('valid.mbtiles');
  var dst = dst.slice(0, dst.indexOf('{z}'));
  copy(fixture, dst, {}, function(err) {
    t.ok(err);
    t.equal(err.message, 'Destination URL does not include a {z}/{x}/{y} template.');
    t.end();
  });
});
