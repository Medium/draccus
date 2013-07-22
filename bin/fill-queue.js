#!/usr/bin/env node

// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Utility that sends junk messages to an SQS queue for testing
 * purposes.
 *
 * Use with same flags/config as draccus.js with the additional flag `--batches`
 */

var flags = require('flags')
flags.defineNumber('batches', 10, 'How many batches of 10 messages to send to the queue')

var AWS = require('aws-sdk')
var options = require('../lib/options')

var sqs = new AWS.SQS(options.getAwsOptions())
sqs.getQueueUrl({'QueueName': options.queueName}, function (err, data) {
  if (err) {
    console.error('Unable to resolve queue "' + options.queueName + '": ' + err.message)
    process.exit(1)
  }

  var queueUrl = data['QueueUrl']
  var batches = flags.get('batches')
  var count = 0

  sendBatch()

  function sendBatch() {
    var entries = []
    for (var i = 0; i < 10; i++, count++) {
      entries.push({'Id': String(count), 'MessageBody': 'Random message numero ' + (count)})
    }

    console.log('Creating messages', count - 10, 'through', count)
    sqs.sendMessageBatch({'QueueUrl': queueUrl, 'Entries': entries}, function (err, data) {
      if (err) {
        console.error(err.stack)
        process.exit(1)
      }
      if (--batches > 0) sendBatch()
      else console.log('All done')
    })
  }

})
