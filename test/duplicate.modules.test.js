var exec = require('child_process').exec;
var test = require('tape').test;

var count_module = function(name,callback) {
  var cmd = 'npm ls ' + name;
  exec(cmd,
    function (error, stdout, stderr) {
      var pattern = new RegExp(name+'@','g');
      var match = stdout.match(pattern);
      if (!match) {
        return callback(null,0);
      }
      return callback(null,match.length);
  });
};
['mapnik', 'tilelive'].forEach(function(mod) {
  test('Duplicate modules: there should only be one ' + mod + ' otherwise you are asking for pwnage', function(t) {
    count_module(mod, function(err,count) {
      if (err) throw err;
      t.notEqual(count, 0, 'includes ' + mod + ' module (`npm ls ' + mod + '`)');
      t.equal(count, 1, 'one copy of ' + mod + ' (`npm ls ' + mod + '`)');
      t.end();
    });
  });
});
