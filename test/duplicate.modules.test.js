var exec = require('child_process').exec;
var test = require('tape').test;

['mapnik', 'tilelive', 'gdal', 'sqlite3'].forEach(function(mod) {
  test('Duplicate modules: there should only be one ' + mod + ' otherwise you are asking for pwnage', function(t) {
    var cmd = 'npm ls ' + mod;
    exec(cmd, function (error, stdout, stderr) {
      var pattern = new RegExp(mod + '@','g');
      var match = stdout.match(pattern);
      t.ok(match && match.length === 1, 'one copy of ' + mod + ' (`npm ls ' + mod + '`)');
      t.end();
    });
  });
});
