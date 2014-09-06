// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Tests for SqsSink, uses a mock implementation of the SQS SDK.
 */

var StdoutStore = require('../lib/StdoutStore')
var SqsSink = require('../lib/SqsSink')

// Don't exit on errors since the mock SQS doesn't swallow errors.
SqsSink.exitOnErrors = false

var pendingTimeouts
var allowMultipleReceivers

SqsSink.setTimeout = function (fn, ms) {
  if (pendingTimeouts.length > 0 && !allowMultipleReceivers) {
    throw new Error('Timeout already scheduled')
  }
  pendingTimeouts.push(fn)
}

function fireTimeout() {
  if (pendingTimeouts.length === 0) throw new Error('No timeout scheduled')
  pendingTimeouts.shift()()
}

function hasPendingTimeouts() {
  return pendingTimeouts.length > 0
}

var collectedMessages
var receiveCount
var mockSqs
var sqsSink

exports.setUp = function (done) {
  collectedMessages = {}
  pendingTimeout = null
  receiveCount = 0
  mockSqs = {}
  pendingTimeouts = []
  allowMultipleReceivers = false

  sqsSink = new SqsSink(mockSqs)
  sqsSink.setQueueUrl('/queue/for/testing')
  sqsSink.on('receive', function (messages) {
    receiveCount++
    for (var key in messages) {
      collectedMessages[key] = messages[key]
    }
  })

  done()
}


exports.testReceive = function (test) {

  var params, callback, requestCount = 0
  mockSqs.receiveMessage = function (p, c) {
    params = p
    callback = c
    requestCount++
  }

  sqsSink.startReceiving()

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

  test.equal(1, receiveCount, 'One set of messages should have been received ')
  test.equal(Object.keys(collectedMessages).length, 2)

  test.equal(1, requestCount, 'Next batch should not be dispatched until handler calls back')

  // Fire the timeout that causes the sink to request more.
  fireTimeout()

  test.equal(2, requestCount)

  callback(null, {
    'Messages': [
      {ReceiptHandle: 'C', Body: '789'},
      {ReceiptHandle: 'D', Body: '0AB'}
    ]
  })

  test.equal(2, receiveCount, 'One set of messages should have been received ')
  test.equal(Object.keys(collectedMessages).join(''), 'ABCD')
  test.equal(collectedMessages['A'], '123')
  test.equal(collectedMessages['B'], '456')
  test.equal(collectedMessages['C'], '789')
  test.equal(collectedMessages['D'], '0AB')

  fireTimeout()

  test.equal(3, requestCount)
  test.equal(sqsSink._emptyPollDelaySec, 2)

  callback(null, {'Messages': []})
  test.equal(sqsSink._emptyPollDelaySec, 4, 'Poll delay should be incremented after empty response')

  test.equal(3, requestCount, 'Another request should not be made after empty response')
  test.equal(2, receiveCount, 'No messages should have been received')

  fireTimeout()
  test.equal(4, requestCount, 'Request should have been made after timeout fires')

  callback(null, {'Messages': []})
  test.equal(sqsSink._emptyPollDelaySec, 6, 'Poll delay should be incremented after empty response')
  fireTimeout()

  callback(null, {'Messages': [{ReceiptHandle: 'E', Body: 'CDE'}]})
  test.equal(sqsSink._emptyPollDelaySec, 2, 'Poll delay should be reset after success')

  test.ok(sqsSink.isReceiving())

  test.done()
}

exports.testMutipleRunningReceivers = function (test) {
  allowMultipleReceivers = true

  var params, callback = [], requestCount = 0
  mockSqs.receiveMessage = function (p, c) {
    callback.push(c)
    requestCount++
  }

  sqsSink.setMaxConcurrentReceivers(2)
  sqsSink.startReceiving()

  test.equal(2, requestCount)

  // Fire the callback.
  callback[0](null, {
    'Messages': [
      {ReceiptHandle: 'A', Body: '123'},
      {ReceiptHandle: 'B', Body: '456'}
    ]
  })
  callback[1](null, {
    'Messages': [
      {ReceiptHandle: 'C', Body: '789'},
      {ReceiptHandle: 'D', Body: '0AB'}
    ]
  })

  test.equal(2, receiveCount, 'Two sets of messages should have been received ')
  test.equal(Object.keys(collectedMessages).join(''), 'ABCD')
  test.equal(collectedMessages['A'], '123')
  test.equal(collectedMessages['B'], '456')
  test.equal(collectedMessages['C'], '789')
  test.equal(collectedMessages['D'], '0AB')

  test.equal(2, requestCount, 'Next batch should not be dispatched until handler calls back')

  // The rest of the tested behaviors should be very similar to the scenario where
  // there is only one maximum waiting receiver. Even if we allow multiple
  // concurrent receivers, they don't know about each other.

  // Fire the timeout (twice!) that causes the sink to request more.
  // We can fire twice because there are two pending timeout at this moment.
  fireTimeout()
  fireTimeout()

  test.equal(4, requestCount)

  callback[2](null, {
    'Messages': [
      {ReceiptHandle: 'E', Body: 'CDE'}
    ]
  })

  test.equal(3, receiveCount, 'Three sets of messages should have been received ')
  test.equal(Object.keys(collectedMessages).join(''), 'ABCDE')
  test.equal(collectedMessages['E'], 'CDE')

  callback[3](null, {
    'Messages': [
      {ReceiptHandle: 'F', Body: 'FGH'}
    ]
  })

  test.equal(4, receiveCount, 'Four sets of messages should have been received ')
  test.equal(Object.keys(collectedMessages).join(''), 'ABCDEF')
  test.equal(collectedMessages['F'], 'FGH')

  fireTimeout()

  test.equal(5, requestCount)
  test.equal(sqsSink._emptyPollDelaySec, 2)

  callback[4](null, {'Messages': []})
  test.equal(sqsSink._emptyPollDelaySec, 4, 'Poll delay should be incremented after empty response')

  test.equal(5, requestCount, 'Another request should not be made after empty response')
  test.equal(4, receiveCount, 'No messages should have been received')

  fireTimeout()
  test.equal(6, requestCount, 'Request should have been made after timeout fires')

  callback[5](null, {'Messages': []})
  test.equal(sqsSink._emptyPollDelaySec, 6, 'Poll delay should be incremented after empty response')
  fireTimeout()

  callback[6](null, {'Messages': [{ReceiptHandle: 'G', Body: 'IJK'}]})
  test.equal(sqsSink._emptyPollDelaySec, 2, 'Poll delay should be reset after success')
  test.equal(Object.keys(collectedMessages).join(''), 'ABCDEFG')

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
  sqsSink.startReceiving()
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

  test.ok(!hasPendingTimeouts(), 'No timeout should be scheduled')

  test.ok(!sqsSink.isReceiving(), 'Sink should have stopped')

  test.done()
}


exports.testReceiveError = function (test) {
  var requestCount = 0
  mockSqs.receiveMessage = function (p, c) {
    requestCount++
    c(new Error('Not a real error, OK to ignore'))
  }

  sqsSink.startReceiving()

  test.equal(requestCount, 1)
  test.ok(hasPendingTimeouts(), 'Timeout should have been set after error')

  fireTimeout()

  test.equal(requestCount, 2)
  test.ok(hasPendingTimeouts(), 'Timeout should have been set after error')

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


