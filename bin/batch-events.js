#!/usr/bin/env node

// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Takes messages from stdin, separated by line breaks and sends
 * them to the queue.
 *
 * Use with same flags/config as draccus.js with the additional flag `--batches`
 */

var now = Date.now()

function generateEventId () {
  return Date.now().toString(36) + Math.round(Math.random() * 1E16).toString(36) 
}

console.log(JSON.stringify({"id":generateEventId(),"type":"emit","client":"web","userAgent":"curl/7.30.0","createdAt":now,"reportedAt":now,"name":"posts.forEachInMedium","value":1,"data":{"emitEvent":"post.magnitudeForTimespan","data":{"startIndex":0,"timespan":"day","overrideTimestamp":now}},"userId":"lo_deadbeefcafe","isAuthenticated":false,"tags":{}}))

console.log(JSON.stringify({"id":generateEventId(),"type":"emit","client":"web","userAgent":"curl/7.30.0","createdAt":now,"reportedAt":now,"name":"homepage.generateFromTimespanMagnitude","value":1,"data":{"timespan":"day","count":200},"userId":"lo_deadbeefcafe","isAuthenticated":false,"tags":{}}))

console.log(JSON.stringify({"id":generateEventId(),"type":"emit","client":"web","userAgent":"curl/7.30.0","createdAt":now,"reportedAt":now,"name":"posts.forEachInMedium","value":1,"data":{"emitEvent":"post.magnitudeForTimespanPostMetrics","data":{"startIndex":0,"overrideTimestamp":now}},"userId":"lo_deadbeefcafe","isAuthenticated":false,"tags":{}}))

console.log(JSON.stringify({"id":generateEventId(),"type":"emit","client":"web","userAgent":"curl/7.30.0","createdAt":now,"reportedAt":now,"name":"collections.forEachInMedium","value":1,"data":{"emitEvent":"collection.recommendedPostAggregation","data":{}},"userId":"lo_deadbeefcafe","isAuthenticated":false,"tags":{}}))

