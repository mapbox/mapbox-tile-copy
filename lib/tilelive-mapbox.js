var TileJSON = require('tilejson');
var url = require('url');

module.exports = Mapbox;

function Mapbox(uri, callback) {
  uri = url.parse(uri);

  if (!process.env.MapboxAPIMaps)
    return callback(new Error('env var MapboxAPIMaps is required'));
  if (!process.env.MapboxAccessToken)
    return callback(new Error('env var MapboxAccessToken is required'));

  uri = process.env.MapboxAPIMaps + '/v4' + uri.pathname + '.json?secure=1&access_token=' + process.env.MapboxAccessToken;
  return new TileJSON(uri, callback);
}

Mapbox.registerProtocols = function(tilelive) {
  tilelive.protocols['mapbox:'] = Mapbox;
};
