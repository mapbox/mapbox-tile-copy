var fs = require('fs');
var util = require('util');
var TileStatStream = require('tile-stat-stream');
var tilelive = require('tilelive');
var zlib = require('zlib');
var url = require('url');
var progress = require('progress-stream');
var S3 = require('tilelive-s3');

var tiletype = require('tiletype');
var mapnik = require('mapnik');
var stream = require('stream');
// var ValidationStream = require('mapbox-upload-validate/lib/validators/serialtiles').ValidationStream;

function serialtiles(srcUri, s3urlTemplate, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
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

    var deserialize = tilelive.deserialize(options)
      .on('error', done)
      .on('tile', function(tile) {
        if (!tile.buffer) return;
        stats.done++;
        if (tile.buffer.length <= max_tilesize) return;
        var err = new Error(util.format('Tile exceeds maximum size of %sk at z %s. Reduce the detail of data at this zoom level or omit it by adjusting your minzoom.', Math.round(max_tilesize / 1024), tile.z));
        err.code = 'EINVALID';
        done(err);
      });

    var validate = new ValidationStream({ 
      validateVectorTiles: true, 
      numTiles: max_tilesize })
      .on('error', function(err) {
        err.code = 'EINVALID';
        done(err);
      });

    if (options.progress) prog.on('progress', function(p) { options.progress(stats, p); });

    if (options.stats) {
      source
        .pipe(gunzip)
        .pipe(deserialize)
        .pipe(validate)
        .pipe(statStream)
        .pipe(prog)
        .pipe(s3);
    } else {
      source
        .pipe(gunzip)
        .pipe(deserialize)
        .pipe(validate)
        .pipe(prog)
        .pipe(s3);

    }
  }

  function done(err) {
    if (once) return;
    once = true;

    if (err) source.unpipe();
    if (options.stats) {
      callback(err, statStream.getStatistics());
    } else {
      callback(err);
    }
  }
}

var ValidationStream = function(options) {
  var validationStream = new stream.Transform({ objectMode: true });
  validationStream.tiles = 0;
  validationStream.max = options.numTiles || Infinity;

  validationStream._transform = function(tile, enc, callback) {
    if (!tile.buffer) return callback();

    if (validationStream.tiles >= validationStream.max) {
      validationStream.push(tile);
      validationStream.tiles++;
      return callback();
    }

    var format = tiletype.type(tile.buffer);
    if (!format) return callback(invalid('Invalid tiletype'));

    // if (!validLength(tile, options.sizeLimit))
    //   return callback(invalid('Tile exceeds maximum size of ' + Math.round(options.sizeLimit / 1024) + 'k at z' + tile.z + '. Reduce the detail of data at this zoom level or omit it by adjusting your minzoom.'));

    // if (!options.validateVectorTiles || format !== 'pbf') {
    //   validationStream.push(tile);
    //   validationStream.tiles++;
    //   return callback();
    // }

    validateVectorTile(tile, function(err) {
      if (err) return callback(err);
      validationStream.push(tile);
      validationStream.tiles++;
      return callback();
    });
  };

  return validationStream;
};

function validateVectorTile(tile, callback) {
  // var vtile = new mapnik.VectorTile(tile.z, tile.x, tile.y);

  // vtile.setData(tile.buffer, function(err) {
    
  //   // Right now we are just sending a generic error response.
  //   // We can return more info from Node Mapnik if we need
  //   // but tests currently check for DeserializationError.
  //   if (err) {
  //     err.name = 'DeserializationError';
  //     err.message = 'Invalid data';
  //     return callback(err);
  //   }

  //   // copying response from mapbox-upload-validate
  //   if (vtile.empty()) {
  //     var error = {
  //       code: 'EINVALID',
  //       message: 'Tile is empty'
  //     };
  //     return callback(error);
  //   }

  //   return callback();
  // });

  // OR WE CAN DO THIS

  var info = mapnik.VectorTile.info(tile.buffer);
  if (info.errors) {
    var error = {
      name: 'DeserializationError',
      message: 'Invalid data'      
    };
    return callback(error);
  }
  return callback();

}

module.exports = serialtiles;
