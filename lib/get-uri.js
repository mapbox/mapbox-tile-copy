var sniffer = require('mapbox-file-sniff');
var path = require('path');

module.exports = function(filepath, layerName, callback) {
  sniffer.quaff(filepath, true, function(err, protocol) {
    if (err) return callback(err);
    var uri = protocol + '//' + path.resolve(filepath);
    if (layerName) uri += '?layerName=' + layerName;
    callback(null, uri);
  });
};
