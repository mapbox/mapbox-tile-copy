var fs = require('fs');
var os = require('os');
var util = require('util');
var TileStatStream = require('./tile-stat-stream');
var tilelive = require('@mapbox/tilelive');
var zlib = require('zlib');
var url = require('url');
var progress = require('progress-stream');
var MigrationStream = require('./migration-stream');
var S3 = require('@mapbox/tilelive-s3');
var utils = require('./utils.js');

function serialtiles(srcUri, s3urlTemplate, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }

  if (options.tileSizeStats) {
    var tileSizeStats = { total: 0, count: 0, max: 0 };
  }

  var statStream = new TileStatStream();

  var max_tilesize = options.limits && options.limits.max_tilesize ?
    options.limits.max_tilesize : 500 * 1024;
  var once;

  var source = fs.createReadStream(url.parse(srcUri).pathname)
    .on('error', done);

  var prog = progress({
    objectMode: true,
    time: 100
  });

  var stats = {
    skipped: 0,
    done: 0
  };

  new S3({
    data: { tiles: [ s3urlTemplate ] },
    sse: 'AES256'
  }, function(err, dst) {
    if (err) return callback(err);
    copy(dst);
  });

  function copy(dst) {
    var gunzip = zlib.createGunzip()
      .on('error', done);

    var s3 = tilelive.createWriteStream(dst, {retry:options.retry})
      .on('error', done)
      .on('stop', done);

    var migrate = MigrationStream()
        .on('error', done);

    var deserialize = tilelive.deserialize(options)
      .on('error', done)
      .on('tile', function(tile) {
        if (!tile.buffer) return;
        stats.done++;
        if (options.tileSizeStats) {
          tileSizeStats.count++;
          tileSizeStats.total = tileSizeStats.total + (tile.buffer.length * 0.001);
          if (tileSizeStats.max < tile.buffer.length) {
            tileSizeStats.max = tile.buffer.length;
          }
          var area = tileSizeStats.hasOwnProperty(tile.z)
            ? tileSizeStats[tile.z] + utils.calculateTileArea(tile.z, tile.x, tile.y)
            : utils.calculateTileArea(tile.z, tile.x, tile.y);

          tileSizeStats = {
            ...tileSizeStats,
            [tile.z]: area
          };

        }
        if (tile.buffer.length <= max_tilesize) return;
        var err = new Error(util.format('Tile exceeds maximum size of %sk at z %s. Reduce the detail of data at this zoom level or omit it by adjusting your minzoom.', Math.round(max_tilesize / 1024), tile.z));
        err.code = 'EINVALID';
        done(err);
      });

    if (options.progress) prog.on('progress', function(p) { options.progress(stats, p); });

    if (options.stats) {
      source
        .pipe(gunzip)
        .pipe(deserialize)
        .pipe(migrate)
        .pipe(statStream)
        .pipe(prog)
        .pipe(s3);
    } else {
      source
        .pipe(gunzip)
        .pipe(deserialize)
        .pipe(migrate)
        .pipe(prog)
        .pipe(s3);
    }
  }

  function done(err) {
    if (once) return;
    once = true;

    if (err) source.unpipe();

    if (options.tileSizeStats) {
      // dump file to tmp dir
      tileSizeStats.avg = tileSizeStats.total / tileSizeStats.count;
      if (tileSizeStats.count > 0) {
        var file = os.tmpdir() + '/tilelive-bridge-stats.json';
        fs.writeFileSync(file, JSON.stringify(tileSizeStats));
      }
    }

    if (options.stats) {
      callback(err, statStream.getStatistics());
    } else {
      callback(err);
    }
  }
}

module.exports = serialtiles;
