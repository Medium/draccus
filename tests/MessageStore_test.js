// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Tests for MessageStore.
 */

var MessageStore = require('../lib/MessageStore')
var EventEmitter = require('events').EventEmitter


var pendingTimeout
MessageStore.setTimeout = function (fn, ms) {
  if (pendingTimeout) throw new Error('Timeout already scheduled')
  pendingTimeout = fn
  return 1234
}


function fireTimeout() {
  if (!pendingTimeout) throw new Error('No timeout scheduled')
  var timeout = pendingTimeout
  pendingTimeout = null
  timeout()
}


var fsFilename, fsData, fsCallback
MessageStore.fs = {
  // Stupid mock-fs that keeps track of the arguments.
  writeFile: function (filename, data, callback) {
    fsFilename = filename
    fsData = data
    fsCallback = callback
  }
}


var messageStore, acknowledgedMessages
exports.setUp = function (done) {
  pendingTimeout = null
  fsFilename = null
  fsData = null
  fsCallback = null
  acknowledgedMessages = []

  var fakeSink = new EventEmitter()
  fakeSink.acknowledge = function (keys) {
    acknowledgedMessages = acknowledgedMessages.concat(keys)
  }

  messageStore = new MessageStore(fakeSink, 'out-dir', 'X')

  done()
}


exports.testHandleMessages = function (test) {
  test.ok(!messageStore.isFlushScheduled(), 'Flush should not yet be scheduled')

  messageStore.handleMessages({'a': '1', 'b': '2', 'c': '3'}, function () {})
  test.ok(messageStore.isFlushScheduled(), 'Flush should now be scheduled')

  fireTimeout()
  test.ok(messageStore.isFlushScheduled(), 'Flush should still be in progress')

  var parts = fsFilename.split('/')
  test.equal(parts[0], 'out-dir', 'Outdir should be set')
  test.ok(/^[0-9]+$/.test(parts[1]), 'File should look like a timestamp')
  test.equal('1\n2\n3\n', fsData, 'Message values should be saved, separated by new lines')

  test.equal(acknowledgedMessages.length, 0, 'No messages should have been acked yet')

  fsCallback(null)

  test.ok(!messageStore.isFlushScheduled(), 'No flush should be in progress anymore')
  test.deepEqual(acknowledgedMessages, ['a', 'b', 'c'], 'Three messages should have been acked')

  test.done()
}


// TODO(dan): Test filenames
// TODO(dan): Test timeout is accurate
// TODO(dan): Test error handling
