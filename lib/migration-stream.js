var stream = require('stream');
var tiletype = require('@mapbox/tiletype');
var mapnik = require('mapnik');

module.exports = MigrationStream;
module.exports.migrate = migrate;

function migrate(tile, callback) {
  var vtile = new mapnik.VectorTile(tile.z, tile.x, tile.y);
  vtile.setData(tile.buffer, {upgrade:true}, function(err) {
    if (err) {
        err.code = 'EINVALID';
        return callback(err);
    }
    vtile.getData({compression:'gzip'},function(err, data) {
      if (err) return callback(err);
      tile.buffer = data;
      return callback(null, tile);
    });
  });
}

function checkForErrors(tile) {
  var info = mapnik.VectorTile.info(tile.buffer);
  var err = null;

  if (info.errors) {
    if (info.tile_errors) {
      err = new Error(info.tile_errors[0].replace(' message', ''));
    } else if (info.layers) {
      for (var i = 0; i < info.layers.length; i++) {
        var layer = info.layers[i];
        if (layer.errors) {
          err = new Error(layer.errors[0].replace(' message', ''));
          break;
        }
      }
    } else {
      err = new Error('Invalid data');
    }
    err.code = 'EINVALID';
  }

  return err;
}

function MigrationStream() {
  var migrationStream = new stream.Transform({ objectMode: true });
  var v1TileDataLogged = false; 

  migrationStream._transform = function(tile, enc, callback) {
    if (!tile.buffer) {
      migrationStream.push(tile);
      return callback();
    }

    var format = tiletype.type(tile.buffer);
    if (format !== 'pbf') {
      migrationStream.push(tile);
      return callback();
    }

    var info = mapnik.VectorTile.info(tile.buffer);

    var v2 = info.layers.every(function(layer) {
        return layer.version === 2;
    });
    
    var randLetter = String.fromCharCode(65 + Math.floor(Math.random() * 26));
    var uniqid = randLetter + Date.now();
    var v1TileData = {'event':'Upload Tile Version', 'anonymousId':uniqid, 'tile_version': null}

    if (v2 === true) {
      // Check for errors on the v2 tile
      var err = checkForErrors(tile);
      if (err) return callback(err);

      migrationStream.push(tile);
      return callback();
    }
    else{
      if(!v1TileDataLogged && process.env.LOG_V1_TILES){
        v1TileData['tile_version'] = 'V1';

        // this will run on require, which means downstream users that are registering plugins
        // and include this environment variable will hit this section even if it is not desired
        process.on('exit', function() {
            if (v1TileData.tile_version) {
                fs.writeFileSync('v1-stats.json', JSON.stringify(v1TileData));
            }
        });
      }; 
      v1TileDataLogged = true;
    }


    migrate(tile, function(err, tile) {
      if (err) return callback(err);

      // Check for errors on newly-minted v2 tile
      var err = checkForErrors(tile);
      if (err) return callback(err);

      migrationStream.push(tile);
      return callback();
    });
  };

  return migrationStream;
}
