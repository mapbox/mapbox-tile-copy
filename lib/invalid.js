var os = require('os');
var path = require('path');
var util = require('util');

module.exports = function invalid(err) {
  var msg = typeof err === 'string' ?
    util.format.apply(this, arguments) : err.message;

  msg = msg
    .replace(new RegExp(path.join(os.tmpdir(),'[0-9a-z]+-'), 'g'), '');

  var error = new Error(msg);
  error.code = 'EINVALID';

  if (err.stack) error.stack = err.stack;
  
  return error;
};
