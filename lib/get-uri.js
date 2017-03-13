var sniffer = require('@mapbox/mapbox-file-sniff');
var path = require('path');

module.exports = function(filepath, layerName, callback) {
  sniffer.fromFile(filepath, function(err, info) {
    if (err) return callback(err);
    var uri = info.protocol + '//' + path.resolve(filepath);
    if (layerName) uri += '?layerName=' + layerName;
    callback(null, uri);
  });
};
