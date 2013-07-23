// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Store that writes messages to disk in batches, useful for dev
 * environments and testing, or for draining a queue to a local file.  Messages
 * will be acknowledged when the write has been confirmed.
 *
 * Also used as a base class for other implementations.
 */

var fs = require('fs')
var path = require('path')
var logg = require('logg')
var moment = require('moment')


/**
 * @param {SqsSink} sink
 * @param {string} outDir Location to store messages
 * @param {string} filenamePattern momentjs compatible string for generating filenames
 * @param {number} flushFreqSec How often to flush messages
 */
function MessageStore(sink, outDir, filenamePattern, flushFreqSec) {

  this._outDir = outDir
  this._filenamePattern = filenamePattern.replace(/PID/, process.pid)
  this._flushFreqSec = flushFreqSec || 60
  this._pendingMessages = {}
  this._flushTimer = null

  this.sink = sink
  this.sink.on('receive', this.handleMessages.bind(this))

  this.logger.info('Writing messages in', this._flushFreqSec, 'second batches')
}
module.exports = MessageStore


/**
 * Alias of typical set timeout, used for testing.
 * @type {Function}
 */
MessageStore.setTimeout = setTimeout


/**
 * Alias of typical node's FS library, used for testing.
 * @type {Object}
 */
MessageStore.fs = fs


MessageStore.prototype.logger = logg.getLogger('MessageStore')


/** @return {boolean} Whether a flush has been scheduled. */
MessageStore.prototype.isFlushScheduled = function () {
  return !!this._flushTimer
}


/**
 * @return {string} Returns a new filename based on the filename pattern and the
 *     current timestamp.  See http://momentjs.com/docs/#/displaying/ for
 *     formatting options.  Additionally PID will be replaced by the process id.
 */
MessageStore.prototype.newFilename = function () {
  return moment().format(this._filenamePattern)
}


/**
 * Handles messages from the sink.
 * @param {Object.<string, string>} messages Map of Receipt Handle to Message Body
 */
MessageStore.prototype.handleMessages = function (messages) {
  if (!this.sink) throw new Error('No sink set')

  for (var key in messages) {
    this._pendingMessages[key] = messages[key]
  }
  if (!this._flushTimer) {
    this._flushTimer = MessageStore.setTimeout(
        this._flush.bind(this), this._flushFreqSec * 1000)
  }
}


/**
 * Writes the data and calls back when the write completes
 * @param {string} data
 * @param {function (Error, string)} callback
 */
MessageStore.prototype.write = function (data, callback) {
  var filename = path.join(this._outDir, this.newFilename())
  // TODO: Create directory if it doesn't exist.
  MessageStore.fs.writeFile(filename, data, function (err) {
    callback(err || null, filename)
  })
}


MessageStore.prototype._flush = function () {
  var messagesToSave = this._pendingMessages
  this._pendingMessages = {}

  var data = '', lines = 0
  for (var key in messagesToSave) {
    data += messagesToSave[key] + '\n'
    lines++
  }

  this.write(data, function (err, identifier) {
    if (err) {
      this.logger.error('Error writing', identifier, err.stack)

      // If there's an error we don't ack _OR_ retry the messages. They will
      // become visible again to the queue and will be fetched again from SQS.
      // An alternative would be to add them back to pendingMessages now and to
      // update the visibility timeout for the message.
      // Another possible improvement is to tell SQS that this message can be
      // made visible immediately rather than waiting for the timeout

    } else {
      this.logger.info(String(lines), 'messages written to', identifier)
      this.sink.acknowledge(Object.keys(messagesToSave))
    }

    this._flushTimer = null
  }.bind(this))
}
