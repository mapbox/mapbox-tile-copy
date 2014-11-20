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
