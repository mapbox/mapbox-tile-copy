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

var init = require('../index.js');
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
    console.log('  --withoutprogress=[boolean]      Default: false');
    process.exit(1);
}

argv.parts = argv.parts || undefined;
argv.part = argv.part || undefined;
argv.withoutprogress = argv.withoutprogress || false;

var srcuri = argv._[0];
var dsturi = argv._[1] ? argv._[1] : false;
var jobs;

if (argv.part && argv.parts) jobs = {
    total:argv.parts,
    num:argv.part
}

init(srcuri, dsturi, jobs);
