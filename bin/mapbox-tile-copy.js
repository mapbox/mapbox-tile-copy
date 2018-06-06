#!/usr/bin/env node

/* eslint no-process-exit: 0, no-path-concat: 0, no-octal-escape: 0 */

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

var util = require('util');
var fs = require('fs');
var http = require('http');
var https = require('https');
http.globalAgent.maxSockets = 30;
https.globalAgent.maxSockets = 30;

var mapboxTileCopy = require('../index.js');
var argv = require('minimist')(process.argv.slice(2));

if (!argv._[0]) {
  process.stdout.write(fs.readFileSync(__dirname + '/help', 'utf8'));
  process.exit(1);
}

var srcfile = argv._[0];
var dsturi = argv._[1];
var options = {};

options.progress = getProgress;

options.stats = !!argv.stats;

['minzoom','maxzoom'].forEach(function(zoomopt) {
  if (!!argv[zoomopt]) {
    if (isNumeric(argv[zoomopt])) {
      options[zoomopt] = argv[zoomopt];
    }
    else {
      console.error('You must provide a valid zoom level integer');
      process.exit(1);
    }
  }
});

if (argv.layerName) options.layerName = argv.layerName;

var interval = argv.progressinterval === undefined ? -1 : Number(argv.progressinterval);

if (interval > 0) {
  setInterval(report, interval * 1000);
}

if (isNumeric(argv.part) && isNumeric(argv.parts)) options.job = {
  total: argv.parts,
  num: argv.part
};

if (isNumeric(argv.retry)) options.retry = parseInt(argv.retry, 10);
if (isNumeric(argv.timeout)) options.timeout = parseInt(argv.timeout, 10);
if (argv.bundle === 'true') options.bundle = true;

if (!dsturi) {
  console.error('You must provide a valid s3:// or file:// url');
  process.exit(1);
}

var srcfile0 = srcfile.split(',')[0];
fs.exists(srcfile0, function(exists) {
  if (!exists) {
    console.error('The file specified does not exist: %s', srcfile);
    process.exit(1);
  }

  if (options.bundle === true) { srcfile = 'omnivore://' + srcfile };
  mapboxTileCopy(srcfile, dsturi, options, function(err, stats) {
    if (err) {
      console.error(err.message);
      process.exit(err.code === 'EINVALID' ? 3 : 1);
    }

    if (argv.stats) {
      fs.writeFile(argv.stats, JSON.stringify(stats), done);
    } else {
      done();
    }

    function done() {
      if (interval !== 0) report(true);
      process.exit(0);
    }
  });
});

var stats, p;

function getProgress(statistics, prog) {
  stats = statistics;
  p = prog;
  if (interval < 0) report();
}

function report(final) {
  if (!stats || !p) return;
  console.log(util.format('%s%s tiles @ %s/s, %s% complete [%ss]%s',
    interval > 0 ? '' : '\r\033[K',
    p.transferred,
    Math.round(p.speed),
    Math.round(p.percentage),
    p.runtime,
    interval > 0 || final ? '\n' : ''
  ));
}

function isNumeric(num) {
  return !isNaN(parseFloat(num));
}
