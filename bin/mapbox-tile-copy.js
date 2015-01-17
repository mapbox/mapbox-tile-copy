#!/usr/bin/env node

// Our goal is a command that can be invoked something like this:
// $ mapbox-tile-copy /path/to/some/file s3://bucket/folder/{z}/{x}/{y} --part=1 --parts=12
//
// We should use exit codes to determine next-steps in case of an error
// - exit 0: success!
// - exit 1: unexpected failure -> retry
// - exit 3: invalid data -> do no retry

// Perform some performance adjustments early on
var maxThreads = Math.ceil(Math.max(4, require('os').cpus().length * 1.5));
process.env.UV_THREADPOOL_SIZE = maxThreads;

var http = require('http');
var https = require('https');
http.globalAgent.maxSockets = 30;
https.globalAgent.maxSockets = 30;

var mapboxTileCopy = require('../index.js');
var util = require('util');
var fs = require('fs');
var s3urls = require('s3urls');
var argv = require('minimist')(process.argv.slice(2));

if (!argv._[0]) {
  console.log('Usage:');
  console.log('  mapbox-tile-copy <src> <dst> [--options]');
  console.log('');
  console.log('Example:');
  console.log('  mapbox-tile-copy orig.mbtiles s3://bucket/prefix/{z}/{x}/{y}');
  console.log('');
  console.log('Options:');
  console.log('  --parts=[number]');
  console.log('  --part=[number]');
  console.log('  --withoutprogress    Shows progress by default');
  process.exit(1);
}

var srcfile = argv._[0];
var dsturi = argv._[1];
var options = {};

if (!argv.withoutprogress) options.progress = report;

if (isNumeric(argv.part) && isNumeric(argv.parts)) options.job = {
  total: argv.parts,
  num: argv.part
};

if (!dsturi || !s3urls.valid(dsturi)) {
  console.error('You must provide a valid S3 url');
  process.exit(1);
}

fs.exists(srcfile, function(exists) {
  if (!exists) {
    console.error('The file specified does not exist: %s', srcfile);
    process.exit(1);
  }

  mapboxTileCopy(srcfile, dsturi, options, function(err){
    if (err) {
      console.error(err.stack);
      process.exit(err.code === 'EINVALID' ? 3 : 1);
    }

    process.stdout.write('\n');
    process.exit(0);
  });
});

function report(stats, p) {
  util.print(util.format('\r\033[K[%s] %s%% %s/%s @ %s/s | ✓ %s □ %s | %s left',
    pad(formatDuration(process.uptime()), 4, true),
    pad((p.percentage).toFixed(4), 8, true),
    pad(formatNumber(p.transferred),6,true),
    pad(formatNumber(p.length),6,true),
    pad(formatNumber(p.speed),4,true),
    formatNumber(stats.done - stats.skipped),
    formatNumber(stats.skipped),
    formatDuration(p.eta)
  ));
}

function formatDuration(duration) {
  var seconds = duration % 60;
  duration -= seconds;
  var minutes = (duration % 3600) / 60;
  duration -= minutes * 60;
  var hours = (duration % 86400) / 3600;
  duration -= hours * 3600;
  var days = duration / 86400;

  return (days > 0 ? days + 'd ' : '') +
    (hours > 0 || days > 0 ? hours + 'h ' : '') +
    (minutes > 0 || hours > 0 || days > 0 ? minutes + 'm ' : '') +
    seconds + 's';
}

function pad(str, len, r) {
  while (str.length < len) str = r ? ' ' + str : str + ' ';
  return str;
}

function formatNumber(num) {
  num = num || 0;
  if (num >= 1e6) {
    return (num / 1e6).toFixed(2) + 'm';
  } else if (num >= 1e3) {
    return (num / 1e3).toFixed(1) + 'k';
  } else {
    return num.toFixed(0);
  }
  return num.join('.');
}

function isNumeric(num) {
  return !isNaN(parseFloat(num));
}
