var sniffer = require('mapbox-file-sniff');
var path = require('path');

module.exports = function(filepath, callback) {
  sniffer.quaff(filepath, true, function(err, protocol) {
    if (err) return callback(err);
    callback(null, protocol + '//' + path.resolve(filepath));
  });
};
