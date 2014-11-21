#!/usr/bin/env node

var init = require('../index.js');
var argv = require('minimist')(process.argv.slice(2));

if (!argv._[0]) {
    console.log('Usage:');
    console.log('  mapbox-tile-copy <src> <dst> [--options]');
    console.log('');
    console.log('Example:');
    console.log('  mapbox-tile-copy orig.mbtiles s3://some/file.mbtiles');
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
