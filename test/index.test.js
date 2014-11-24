var test = require('tape').test;
var path = require('path');
var init = require('../index.js');
var util = require('util');
var S3 = require('tilelive-s3');
var tilelive = require('tilelive');
S3.registerProtocols(tilelive);

var srcuri = __dirname + '/fixtures/valid.mbtiles';
var dsturi = 's3://tilestream-tilesets-development/carol-staging/test-mapbox-tile-copy/{z}/{x}/{y}.png';
var options = {};

test('index.js init', function(t) {
	init(srcuri, dsturi, options, function(err){
    	t.end();
	});
});
