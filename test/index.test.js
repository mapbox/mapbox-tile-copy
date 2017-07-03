var test = require('tape').test;
var path = require('path');
var crypto = require('crypto');
var copy = require('../index.js');
// var AWS = require('aws-sdk');
var AWS = require('@mapbox/mock-aws-sdk-js');
var rimraf = require('rimraf');
var mkdirp = require('mkdirp');
var s3urls = require('@mapbox/s3urls');

process.env.MapboxAPIMaps = 'https://api.tiles.mapbox.com';

var runid = crypto.randomBytes(16).toString('hex');

console.log('---> mapbox-tile-copy index %s', runid);

function dsturi(name) {
  return [
    's3:/',
    'mapbox-tile-copy-test',
    runid,
    name,
    '{z}/{x}/{y}'
  ].join('/');
}

function tileCount(dst, callback) {
  var s3 = AWS.S3();

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

test('setup', function(t) {
  mkdirp(`test/tmp-buckets/${runid}`, function(err) {
    t.ifError(err);
    t.end();
  });
});

test('serialtiles', function(t) {
  // AWS.config.accessKeyId = 'test-access-key';
  // AWS.config.secretAccessKey = 'test-secret-key';
  process.env.AWS_ACCESS_KEY_ID = 'test-access-key';
  process.env.AWS_SECRET_ACCESS_KEY = 'test-secret-key';

  AWS.stub('S3', 'putObject', function(params, callback) {
    console.log('stubbing s3 now');
    console.log('mock s3 params:', params);
    return callback(null, { waka: 'flocka' });
  });

  AWS.stub('S3', 'getObject', function(params, callback) {
    console.log('mock s3 params:', params);
    return callback(null, { waka: 'flocka' });
  });


  var fixture = path.resolve(__dirname, 'fixtures', 'valid.serialtiles.gz');
  var dst = dsturi('valid.serialtiles');
  console.log(dst);
  copy(fixture, dst, {}, function(err) {
    console.log('error: ', err.stack);

    t.ifError(err, 'copied');

    AWS.S3.restore();
    t.end();
    // tileCount(dst, function(err, count) {
    //   t.ifError(err, 'counted tiles');
    //   t.equal(count, 4, 'expected number of tiles');
    //   t.end();
    // });
  });
});

// test('mbtiles', function(t) {
//   var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
//   var dst = dsturi('valid.mbtiles');
//   copy(fixture, dst, {}, function(err) {
//     t.ifError(err, 'copied');
//     tileCount(dst, function(err, count) {
//       t.ifError(err, 'counted tiles');
//       t.equal(count, 21, 'expected number of tiles');
//       t.end();
//     });
//   });
// });
//
// test('corrupt mbtiles', function(t) {
//   var fixture = path.resolve(__dirname, 'fixtures', 'invalid.corrupt.mbtiles');
//   var dst = dsturi('invalid.corrupt.mbtiles');
//   copy(fixture, dst, {}, function(err) {
//     t.ok(err, 'expected error');
//     t.equal(err.code, 'EINVALID', 'marked invalid');
//     tileCount(dst, function(err, count) {
//       t.ifError(err, 'counted tiles');
//       t.equal(count, 0, 'rendered no tiles');
//       t.end();
//     });
//   });
// });
//
// test('null mbtiles', function(t) {
//   var fixture = path.resolve(__dirname, 'fixtures', 'invalid.null-tile.mbtiles');
//   var dst = dsturi('invalid.null-tile.mbtiles');
//   copy(fixture, dst, {}, function(err) {
//     t.ok(err, 'expected error');
//     t.equal(err.code, 'EINVALID', 'marked invalid');
//     tileCount(dst, function(err, count) {
//       t.ifError(err, 'counted tiles');
//       t.equal(count, 0, 'rendered no tiles');
//       t.end();
//     });
//   });
// });
//
// test('fails with missing {z}/{x}/{y} template', function(t) {
//   var fixture = path.resolve(__dirname, 'fixtures', 'valid.mbtiles');
//   var dst = dsturi('valid.mbtiles');
//   var dst = dst.slice(0, dst.indexOf('{z}'));
//   copy(fixture, dst, {}, function(err) {
//     t.ok(err);
//     t.equal(err.message, 'Destination URL does not include a {z}/{x}/{y} template.');
//     t.end();
//   });
// });
//
// test('cleanup', function(t) {
//   console.log(__dirname);
//   rimraf(__dirname + '/tmp-buckets', function(err) {
//     t.ifError(err);
//     t.end();
//   });
// });
