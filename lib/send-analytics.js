#!/usr/bin/env node

var Analytics = require('analytics-node');
var minimist = require('minimist');

function sendAnalytics(args,callback) {
  var token = process.env.SegmentioSecret; 
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
    anonymousId:args.anonymousId,
    event: args.event,
    properties: properties
  }, function(err) {
    if (err) return callback(`Error tracking analytics: ${err}`);
    return callback(null, 'Event successfully tracked');
  });
}

module.exports = {
  sendAnalytics: sendAnalytics
};