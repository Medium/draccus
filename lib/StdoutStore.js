// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Simple Message Store implementation that logs messages to the
 * console and acknowledges them instantly.
 */

var logger = require('logg').getLogger('StdoutStore')


/**
 * @constructor
 */
function StdoutStore() {}
module.exports = StdoutStore


StdoutStore.prototype.handleMessages = function (messages, done) {
  var handles = Object.keys(messages)
  handles.forEach(function (key) { logger.info(messages[key]) })
  this.sink.acknowledge(handles)
  done()
}
