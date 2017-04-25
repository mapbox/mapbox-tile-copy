var test = require('tape');
var exec = require('child_process').exec;
var os = require('os');
var fs = require('fs');
var crypto = require('crypto');
var path = require('path');

process.env.MapboxAPIMaps = 'https://api.tiles.mapbox.com';
var copy = path.resolve(__dirname, '..', 'bin', 'mapbox-tile-copy.js');
var bucket = process.env.TestBucket || 'tilestream-tilesets-development';
var runid = crypto.randomBytes(16).toString('hex');
var fixture = path.resolve(__dirname, 'fixtures', 'valid.serialtiles.gz');
var s3urls = require('@mapbox/s3urls');
var AWS = require('aws-sdk');


console.log('---> mapbox-tile-copy executable %s', runid);

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

  var params = s3urls.fromUrl(dst.replace('{z}/{x}/{y}', ''));
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

test('invalid s3 url', function(t) {
  var dst = 'http://www.google.com';
  var cmd = [ copy, fixture, dst ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ok(err, 'expected error');
    t.equal(stderr, 'You must provide a valid S3 url\n', 'expected message');
    t.equal(err.code, 1, 'exit code 1');
    t.end();
  });
});

test('file does not exist', function(t) {
  var dst = dsturi('invalid.file');
  var cmd = [ copy, '/w/t/f', dst ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ok(err, 'expected error');
    t.equal(stderr, 'The file specified does not exist: /w/t/f\n', 'expected message');
    t.equal(err.code, 1, 'exit code 1');
    t.end();
  });
});

test('invalid source file', function(t) {
  var dst = dsturi('invalid.source');
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.tilejson');
  var cmd = [ copy, fixture, dst ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ok(err, 'expected error');
    t.ok(/Unknown filetype/.test(stderr), 'expected message');
    t.equal(err.code, 3, 'exit code 3');
    t.end();
  });
});

test('handles mbtile with missing geometry', function(t) {
  var fixture = path.resolve(__dirname, 'fixtures', 'invalid.tile-with-no-geometry.mbtiles');
  var dst = dsturi('invalid.tile-with-no-geometry.mbtiles');
  var cmd = [ copy, fixture, dst ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'no error');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 1, 'expected number of tiles');
      t.end();
    });
  });
});

test('stats flag', function(t) {
  var dst = dsturi('valid.geojson');
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var tmpfile = path.join(os.tmpdir(), crypto.randomBytes(8).toString('hex'));
  var cmd = [ copy, fixture, '--stats=' + tmpfile, dst ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'no error');
    t.pass(tmpfile);
    var stats = JSON.parse(fs.readFileSync(tmpfile));
    t.ok(stats);
    t.equal(Math.abs(15590 - stats.valid.geometryTypes.Polygon) < 200, true, 'Counts polygons (+/-15590)');
    t.end();
  });
});

test('minzoom flag valid', function(t) {
  var dst = dsturi('valid.mini.geojson');
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mini.geojson');
  var cmd = [ copy, fixture, dst, '--minzoom', '5' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'no error');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 2, 'expected number of tiles');
      t.end();
    });
  });
});

test('minzoom flag nullval', function(t) {
  var dst = dsturi('valid.geojson');
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var cmd = [ copy, fixture, dst, '--minzoom' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ok(err, 'expected error');
    t.ok(/You must provide a valid zoom level integer/.test(stderr), 'expected message');
    t.equal(err.code, 1, 'exit code 1');
    t.end();
  });
});

test('minzoom flag badval', function(t) {
  var dst = dsturi('valid.geojson');
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var cmd = [ copy, fixture, dst, '--minzoom q' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ok(err, 'expected error');
    t.ok(/You must provide a valid zoom level integer/.test(stderr), 'expected message');
    t.equal(err.code, 1, 'exit code 1');
    t.end();
  });
});

test('maxzoom flag valid', function(t) {
  var dst = dsturi('valid.mini.geojson');
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mini.geojson');
  var cmd = [ copy, fixture, dst, '--maxzoom', '4' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'no error');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 5, 'expected number of tiles');
      t.end();
    });
  });
});

test('maxzoom flag nullval', function(t) {
  var dst = dsturi('valid.geojson');
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var cmd = [ copy, fixture, dst, '--maxzoom' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ok(err, 'expected error');
    t.ok(/You must provide a valid zoom level integer/.test(stderr), 'expected message');
    t.equal(err.code, 1, 'exit code 1');
    t.end();
  });
});

test('maxzoom flag badval', function(t) {
  var dst = dsturi('valid.geojson');
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geojson');
  var cmd = [ copy, fixture, dst, '--maxzoom q' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ok(err, 'expected error');
    t.ok(/You must provide a valid zoom level integer/.test(stderr), 'expected message');
    t.equal(err.code, 1, 'exit code 1');
    t.end();
  });
});

test('s3 url', function(t) {
  var dst = dsturi('valid.s3url');
  var cmd = [ copy, fixture, dst ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'copied');
    t.equal(stdout.length, 77, 'expected stdout.length');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 4, 'expected number of tiles');
      t.end();
    });
  });
});

test('https s3 url', function(t) {
  var dst = dsturi('valid.httpurl');
  var cmd = [ copy, fixture, s3urls.convert(dst, 'bucket-in-host') ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'copied');
    t.equal(stdout.length, 77, 'expected stdout.length');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 4, 'expected number of tiles');
      t.end();
    });
  });
});

test('no progress', function(t) {
  var dst = dsturi('valid.noprogress');
  var cmd = [ copy, fixture, dst, '--progressinterval', '0' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'copied');
    t.equal(stdout.length, 0, 'expected stdout.length');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 4, 'expected number of tiles');
      t.end();
    });
  });
});

test('progress interval', function(t) {
  var dst = dsturi('valid.interval');
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.geotiff.tif');
  var cmd = [ copy, fixture, dst, '--progressinterval', '1' ].join(' ');
  var proc = exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'copies');
    t.ok(stdout.length > 0, 'logs something');
    t.end();
  });
});

test('parallel', function(t) {
  var dst = dsturi('valid.parallel');
  var cmd = [ copy, fixture, dst, '--part', '1', '--parts', '10' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'copied');
    t.equal(stdout.length, 77, 'expected stdout.length');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.ok(count < 4, 'did not render all tiles');
      t.end();
    });
  });
});

test('part zero', function(t) {
  var dst = dsturi('valid.part.zero');
  var cmd = [ copy, fixture, dst, '--part', '0', '--parts', '10' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'copied');
    t.equal(stdout.length, 77, 'expected stdout.length');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.ok(count < 4, 'did not render all tiles');
      t.end();
    });
  });
});

test('single part', function(t) {
  var dst = dsturi('valid.single.zero');
  var cmd = [ copy, fixture, dst, '--part', '0', '--parts', '1' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'copied');
    t.equal(stdout.length, 77, 'expected stdout.length');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.ok(count == 4, 'rendered all tiles');
      t.end();
    });
  });
});

test('retry', function(t) {
  var dst = dsturi('valid.retry');
  var cmd = [ copy, fixture, dst, '--retry', '5' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'copied');
    t.equal(stdout.length, 77, 'expected stdout.length');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 4, 'expected number of tiles');
      t.end();
    });
  });
});

test('bundle true', function(t) {
  var dst = dsturi('valid.bundle.true');
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.bundle-layer-1.geojson') + ',' + path.resolve(__dirname, 'fixtures', 'valid.bundle-layer-2.geojson');
  var cmd = [ copy, fixture, dst, '--bundle', true ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'copied');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 167, 'expected number of tiles');
      t.end();
    });
  });
});

test('layerName', function(t) {
  var dst = dsturi('valid.mini.geojson');
  var fixture = path.resolve(__dirname, 'fixtures', 'valid.mini.geojson');
  var cmd = [ copy, fixture, dst, '--layerName', 'named' ].join(' ');
  exec(cmd, function(err, stdout, stderr) {
    t.ifError(err, 'copied');
    t.equal(stdout.length, 77, 'expected stdout.length');
    tileCount(dst, function(err, count) {
      t.ifError(err, 'counted tiles');
      t.equal(count, 5, 'expected number of tiles');
      t.end();
    });
  });
});
