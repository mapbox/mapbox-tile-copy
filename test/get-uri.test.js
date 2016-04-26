var test = require('tape').test;
var path = require('path');
var getUri = require('../lib/get-uri');

[
  'valid.geojson',
  'valid.mbtiles',
  'valid.serialtiles.gz',
  'valid.tilejson',
  'valid.tm2z'
].forEach(function(filename) {
  test('get-uri: ' + filename, function(t) {
    var filepath = path.resolve(__dirname, 'fixtures', filename);
    var expected = {
      geojson: 'omnivore://' + filepath,
      mbtiles: 'mbtiles://' + filepath,
      serialtiles: 'serialtiles://' + filepath,
      tilejson: 'tilejson://' + filepath,
      tm2z: 'tm2z://' + filepath
    }[filename.split('.')[1]];

    getUri(filepath, null, function(err, uri) {
      t.ifError(err, 'got uri');
      t.equal(uri, expected, 'got expected uri');
      t.end();
    });
  });
});

test('get-uri: invalid.tilejson', function(t) {
  var filepath = path.resolve(__dirname, 'fixtures', 'invalid.tilejson');
  getUri(filepath, null, function(err, uri) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'EINVALID', 'expected error code');
    t.equal(err.message, 'Unknown filetype', 'expected error message');
    t.end();
  });
});

test('get-uri: bunk filepath', function(t) {
  getUri('the cheese is old and moldy', null, function(err, uri) {
    t.ok(err, 'expected error');
    t.equal(err.code, 'ENOENT', 'expected error');
    t.end();
  });
});
