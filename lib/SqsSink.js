// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Class that queries SQS for messages
 */

var util = require('util')
var EventEmitter = require('events').EventEmitter
var logger = require('logg').getLogger('SqsSink')


/**
 * @param {AWS.SQS} sqs Amazon SQS Client.
 * @param {boolean} logRawMessage Whether to log the raw SQS message instead of
 *     the message body.
 * @constructor
 */
function SqsSink(sqs, logRawMessage) {
  EventEmitter.call(this)

  /** The AWS.SDK object. */
  this._sqs = sqs

  /** Whether to log the raw SQS message instead of the message body. */
  this._logRawMessage = logRawMessage

  /** Whether to stop receiving messages when the queue is empty. */
  this._stopWhenEmpty = false

  /** The full URL of the SQS queue. */
  this._queueUrl = ''

  /** Max messages to get from SQS in each batch.  (10 is max allowed by SDK) */
  this._maxMessages = 10

  /** How long to wait for if there are no messages currently in the queue. */
  this._waitTimeSec = 10

  /**
   * Specifies how long to SQS should wait for messages to be acked before
   * allowing them to be processed again.  This should be set high enough for
   * the buffering step to process the messages.
   */
  this._visibilityTimeSec = 90

  /** Delay before trying again, if there are no messages. */
  this._emptyPollDelaySec = 0

  /** Counter tracking how many messages have been received. */
  this._totalReceived = 0

  /** Counter tracking how many messages have been acknowledged. */
  this._totalAcked = 0

  /** Whether the sink is already polling for messages. */
  this._receiving = false
}
util.inherits(SqsSink, EventEmitter)
module.exports = SqsSink


/**
 * Whether to exit on errors that are caught.
 * @type {boolean}
 */
SqsSink.exitOnErrors = true


/**
 * Alias of typical set timeout, used for testing.
 * @type {Function}
 */
SqsSink.setTimeout = setTimeout


/**
 * Maximum time to wait between polling, when the queue was empty.
 * @type {number}
 */
SqsSink.MAX_EMPTY_POLL_DELAY_SEC = 15


/**
 * Sets whether the Sink should stop waiting as soon as an empty queue is
 * detected. If true the process will run until terminated.
 * @param {boolean} stopWhenEmpty
 * @return {SqsSink}
 */
SqsSink.prototype.setStopWhenEmpty = function (stopWhenEmpty) {
  this._stopWhenEmpty = stopWhenEmpty
  return this
}


/**
 * Specifies how long to messages should be invisible for while waiting for
 * them to be acknowledged.
 * @param {number} visibilityTimeSec
 * @return {SqsSink}
 */
SqsSink.prototype.setVisibilityTimeSec = function (visibilityTimeSec) {
  this._visibilityTimeSec = visibilityTimeSec
  return this
}


/**
 * Sets the URL of the queue to receive messages from.
 * @param {string} queueUrl
 * @return {SqsSink}
 */
SqsSink.prototype.setQueueUrl = function (queueUrl) {
  this._queueUrl = queueUrl
  return this
}


/**
 * @return {boolean} Whether the sink is polling for messages.
 */
SqsSink.prototype.isReceiving = function () {
  return this._receiving
}


/**
 * Acknowledges that the messages with the given receipt handles have been
 * processed and should be removed from the queue.
 * @param {Array.<string>} receiptHandles
 */
SqsSink.prototype.acknowledge = function (receiptHandles) {
  while (receiptHandles.length !== 0) {
    // SQS requires that messages are deleted in batches of 10.
    this._acknowledgeInternal(receiptHandles.splice(0, 10))
  }
}


/**
 * Starts receiving messages from the configured queue.
 */
SqsSink.prototype.startReceiving = function () {
  if (this._receiving) return
  logger.info('Checking SQS for messages: ' + this._queueUrl)
  this._receiving = true
  this._receiveInternal()
}


SqsSink.prototype._receiveInternal = function () {
  this._sqs.receiveMessage({
    'QueueUrl': this._queueUrl,
    'MaxNumberOfMessages': this._maxMessages,
    'WaitTimeSeconds': this._waitTimeSec,
    'VisibilityTimeout': Math.ceil(this._visibilityTimeSec)
  }, this._wrap(this._onReceive))
}


SqsSink.prototype._acknowledgeInternal = function (receiptHandles) {
  this._sqs.deleteMessageBatch({
    'QueueUrl': this._queueUrl,
    'Entries': receiptHandles.map(function (receiptHandle, index) {
      return {'Id': String(index), 'ReceiptHandle': receiptHandle}
    })
  }, this._wrap(this._onAcknowledge))
}


SqsSink.prototype._onAcknowledge = function (err, data) {
  if (err) {
    logger.error(err)
    if (data) {
      this._totalAcked += data['Successful'].length
      logger.info(data['Successful'].length + ' messages removed from queue')
      logger.info(data['Failed'].length + ' messages failed to be removed')
      logger.info(data['Failed'])

      // TODO: Keep track of messages that should have been acked but failed for
      // a reason not SenderFault and retry them next time.
    }
  } else {
    this._totalAcked += data['Successful'].length
    logger.info(data['Successful'].length + ' messages removed from queue (' +
        this._totalAcked + ' total)')
  }
}


SqsSink.prototype._onReceive = function (err, data) {
  if (err) {
    logger.error('Failed receiving messages', err.statusCode, err.stack)
    // TODO(dan): Add max retries for non-daemon mode.
    this._receiveDelayed()

  } else if (!data['Messages'] || data['Messages'].length === 0) {
    logger.info('No messages received')

    // If no messages came back and the sink is configured to stop when empty,
    // then query the queue to see if there are actually any messages available.
    if (this._stopWhenEmpty) {
      this._sqs.getQueueAttributes({
        'QueueUrl': this._queueUrl,
        'AttributeNames': [
          'ApproximateNumberOfMessages',
          'ApproximateNumberOfMessagesNotVisible',
          'ApproximateNumberOfMessagesDelayed']
      }, this._wrap(this._onQueryAttributes))

    } else {
      this._receiveDelayed()
    }

  } else {
    this._totalReceived += data['Messages'].length

    logger.info(data['Messages'].length + ' messages received (' +
        this._totalReceived + ' total)')

    var messages = {}
    var logRawMessage = this._logRawMessage
    data['Messages'].forEach(function (message) {
      messages[message['ReceiptHandle']] = logRawMessage ? JSON.stringify(message) : message['Body']
    })

    this._emptyPollDelaySec = 0

    this.emit('receive', messages)
    this._receiveDelayed()
  }
}


SqsSink.prototype._onQueryAttributes = function (err, data) {
  if (err) {
    logger.error('Failed querying attributes', err)
    // If the query failed, try the queue again for more messages.
    this._receiveDelayed()
  } else {

    var messageCount = Number(data['Attributes']['ApproximateNumberOfMessages'] || 0)
    var notVisibleCount = Number(data['Attributes']['ApproximateNumberOfMessagesNotVisible'] || 0)
    var delayedCount = Number(data['Attributes']['ApproximateNumberOfMessagesDelayed'] || 0)

    if (messageCount != 0) {
      logger.info(messageCount + ' messages still in the queue, retrying')
      this._receiveDelayed()

    } else if (delayedCount != 0) {
      logger.info(delayedCount + ' delayed messages, waiting')
      this._receiveDelayed()

    } else if (notVisibleCount != 0) {
      logger.info(notVisibleCount + ' invisible messages, waiting')
      this._receiveDelayed()

    } else {
      logger.info('The queue is empty, stopping polling (' +
          this._totalReceived + ' received, ' +
          this._totalAcked + ' acknowledged)')
      this._receiving = false
    }
  }
}


/**
 * Calls receive() in a timer, each call will be 2-seconds slower than
 * the last until MAX_EMPTY_POLL_DELAY_SEC is reached.
 */
SqsSink.prototype._receiveDelayed = function () {
  SqsSink.setTimeout(this._wrap(this._receiveInternal), this._emptyPollDelaySec * 1000)

  // Linear back-off when the queue is empty or there are errors.
  this._emptyPollDelaySec =
      Math.min(SqsSink.MAX_EMPTY_POLL_DELAY_SEC, this._emptyPollDelaySec + 2)
}


/**
 * The AWS SDK appears to swallow JS errors in callbacks, so wrap functions and
 * fail hard if unexpected errors occur.
 * @param {Function} fn
 * @return {Function}
 */
SqsSink.prototype._wrap = function (fn) {
  var self = this
  return function () {
    try {
      return fn.apply(self, arguments)
    } catch (e) {
      logger.error(e.stack)
      if (SqsSink.exitOnErrors) process.exit(1)
      else throw e
    }
  }
}
