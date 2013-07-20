// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Tests for SqsSink, uses a mock implementation of the SQS SDK.
 */

var StdoutStore = require('../lib/StdoutStore')
var SqsSink = require('../lib/SqsSink')

// Don't exit on errors since the mock SQS doesn't swallow errors.
SqsSink.exitOnErrors = false


var pendingTimeout
SqsSink.setTimeout = function (fn, ms) {
  if (pendingTimeout) throw new Error('Timeout already scheduled')
  pendingTimeout = fn
}


function fireTimeout() {
  if (!pendingTimeout) throw new Error('No timeout scheduled')
  var timeout = pendingTimeout
  pendingTimeout = null
  timeout()
}

var collectedMessages
var receiveNext
var mockSqs
var sqsSink


exports.setUp = function (done) {
  collectedMessages = {}
  receiveNext = null

  mockSqs = {}

  sqsSink = new SqsSink(mockSqs)
  sqsSink.setQueueUrl('/queue/for/testing')
  sqsSink.setStore({handleMessages: function (messages, callback) {
    for (var key in messages) {
      collectedMessages[key] = messages[key]
    }
    receiveNext = callback
  }})

  done()
}


exports.testReceive = function (test) {

  var params, callback, requestCount = 0
  mockSqs.receiveMessage = function (p, c) {
    params = p
    callback = c
    requestCount++
  }

  sqsSink.receive()

  test.equal(1, requestCount)

  // Make sure the params are set properly.
  test.equal(params.QueueUrl, '/queue/for/testing')
  test.equal(params.MaxNumberOfMessages, 10)
  test.equal(params.WaitTimeSeconds, 10)
  test.equal(params.VisibilityTimeout, 90)

  // Fire the callback.
  callback(null, {
    'Messages': [
      {ReceiptHandle: 'A', Body: '123'},
      {ReceiptHandle: 'B', Body: '456'}
    ]
  })

  // Two messages should have been collected by the handler.
  test.equal(Object.keys(collectedMessages).length, 2)

  test.equal(1, requestCount, 'Next batch should not be dispatched until handler calls back')

  // Fire the callback passed to the message handler.
  receiveNext()
  receiveNext = null

  test.equal(2, requestCount)

  callback(null, {
    'Messages': [
      {ReceiptHandle: 'C', Body: '789'},
      {ReceiptHandle: 'D', Body: '0AB'}
    ]
  })

  test.equal(Object.keys(collectedMessages).join(''), 'ABCD')
  test.equal(collectedMessages['A'], '123')
  test.equal(collectedMessages['B'], '456')
  test.equal(collectedMessages['C'], '789')
  test.equal(collectedMessages['D'], '0AB')

  receiveNext()
  receiveNext = null

  test.equal(3, requestCount)
  test.equal(sqsSink._emptyPollDelaySec, 0)

  callback(null, {'Messages': []})
  test.equal(sqsSink._emptyPollDelaySec, 2, 'Poll delay should be incremented after empty response')

  test.equal(3, requestCount, 'Another request should not be made after empty response')
  test.equal(receiveNext, null, 'Message handler should not have been called for empty response')

  fireTimeout()
  test.equal(4, requestCount, 'Request should have been made after timeout fires')

  callback(null, {'Messages': []})
  test.equal(sqsSink._emptyPollDelaySec, 4, 'Poll delay should be incremented after empty response')
  fireTimeout()

  callback(null, {'Messages': [{ReceiptHandle: 'E', Body: 'CDE'}]})
  test.equal(sqsSink._emptyPollDelaySec, 0, 'Poll delay should be reset after success')

  test.ok(sqsSink.isReceiving())

  test.done()
}


exports.testReceiveUntilEmpty = function (test) {
  var receiveCallback, getQueueParams, getQueueCallback

  mockSqs.receiveMessage = function (p, c) { receiveCallback = c }
  mockSqs.getQueueAttributes = function (p, c) {
    getQueueParams = p
    getQueueCallback = c
  }

  sqsSink.setStopWhenEmpty(true)
  sqsSink.receive()
  receiveCallback(null, {'Messages': []})
  receiveCallback = null

  test.deepEqual(getQueueParams, {
    'QueueUrl': '/queue/for/testing',
    'AttributeNames': [
      'ApproximateNumberOfMessages',
      'ApproximateNumberOfMessagesNotVisible',
      'ApproximateNumberOfMessagesDelayed'
    ]
  })

  test.ok(sqsSink.isReceiving())

  getQueueCallback(null, {
    'Attributes': {
      'ApproximateNumberOfMessages': 0,
      'ApproximateNumberOfMessagesNotVisible': 1,
      'ApproximateNumberOfMessagesDelayed': 0
    }
  })
  getQueueCallback = null

  test.equal(receiveCallback, null)

  test.ok(sqsSink.isReceiving())

  // If there are potential messages in the queue it shouldn't exit yet,
  // but there should be a timeout scheduled.
  fireTimeout()

  receiveCallback(null, {'Messages': []})
  getQueueCallback(null, {'Attributes': {}})

  test.equal(pendingTimeout, null, 'No timeout should be scheduled')

  test.ok(!sqsSink.isReceiving(), 'Sink should have stopped')

  test.done()
}


exports.testReceiveError = function (test) {
  var requestCount = 0
  mockSqs.receiveMessage = function (p, c) {
    requestCount++
    c(new Error('Not a real error, OK to ignore'))
  }

  sqsSink.receive()

  test.equal(requestCount, 1)
  test.notEqual(pendingTimeout, null, 'Timeout should have been set after error')

  fireTimeout()

  test.equal(requestCount, 2)
  test.notEqual(pendingTimeout, null, 'Timeout should have been set after error')

  test.done()
}


exports.testAcknowledge = function (test) {
  var params = [], callbacks = []
  mockSqs.deleteMessageBatch = function (p, c) {
    params.push(p), callbacks.push(c)
  }

  sqsSink.acknowledge(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O'])

  test.deepEqual(params[0], {
    'QueueUrl': '/queue/for/testing',
    'Entries': [
      {'Id': '0', 'ReceiptHandle': 'A'},
      {'Id': '1', 'ReceiptHandle': 'B'},
      {'Id': '2', 'ReceiptHandle': 'C'},
      {'Id': '3', 'ReceiptHandle': 'D'},
      {'Id': '4', 'ReceiptHandle': 'E'},
      {'Id': '5', 'ReceiptHandle': 'F'},
      {'Id': '6', 'ReceiptHandle': 'G'},
      {'Id': '7', 'ReceiptHandle': 'H'},
      {'Id': '8', 'ReceiptHandle': 'I'},
      {'Id': '9', 'ReceiptHandle': 'J'}
    ]
  })

  test.deepEqual(params[1], {
    'QueueUrl': '/queue/for/testing',
    'Entries': [
      {'Id': '0', 'ReceiptHandle': 'K'},
      {'Id': '1', 'ReceiptHandle': 'L'},
      {'Id': '2', 'ReceiptHandle': 'M'},
      {'Id': '3', 'ReceiptHandle': 'N'},
      {'Id': '4', 'ReceiptHandle': 'O'}
    ]
  })

  callbacks[0](null, {'Successful': [{'Id': '0'}, {'Id': '1'}, {'Id': '2'}, {'Id': '3'},
    {'Id': '4'}, {'Id': '5'}, {'Id': '6'}, {'Id': '7'}, {'Id': '8'}, {'Id': '9'}]})

  callbacks[1](null, {'Successful': [{'Id': '0'}, {'Id': '1'}, {'Id': '2'}, {'Id': '3'},
    {'Id': '4'}]})

  // We don't actually do anything with the response, yet, other than log, so
  // we're just testing for errors.

  test.done()
}


