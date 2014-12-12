var os = require('os');
var path = require('path');
var util = require('util');

module.exports = function invalid(err) {
  if (typeof err === 'string')
    err = new Error(util.format.apply(this, arguments));

  err.code = 'EINVALID';
  return err;
};
