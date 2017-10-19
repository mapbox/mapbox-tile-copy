#!/usr/bin/env node

var Analytics = require('analytics-node');
var minimist = require('minimist');

if (require.main === module) {
  var args = minimist(process.argv.slice(2));
  sendAnalytics(args, process.env.SegmentioSecret, function(code, message) {
    if (code && message) console.error(message);
    process.exit(code);
  });
}

function sendAnalytics(args,callback) {
  if (!token) return callback('Segment Token is required');
  if (!args.event) return callback('Event is required and must match a segmentio event');

  // build properties object
  var properties = {};
  for (var arg in args) {
    if (arg !== 'event') {
      properties[arg] = args[arg];
    }
  }

  // set up analytics controller
  var analytics = new Analytics(token, { flushAt: 1 });

  analytics.track({
    event: args.event,
    properties: properties
  }, function(err) {
    if (err) return callback(1, `Error tracking analytics: ${err}`);
    return callback(0, 'Event successfully tracked');
  });
}

module.exports = {
  sendAnalytics: sendAnalytics
};