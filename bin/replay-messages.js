#!/usr/bin/env node

// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Takes messages from stdin, separated by line breaks and sends
 * them to the queue.
 *
 * Use with same flags/config as draccus.js with the additional flag `--batches`
 */

var AWS = require('aws-sdk')
var options = require('../lib/options')

var sqs = new AWS.SQS(options)
sqs.getQueueUrl({'QueueName': options.queueName}, function (err, data) {
  if (err) {
    console.error('Unable to resolve queue "' + options.queueName + '": ' + err.message)
    process.exit(1)
  }

  var queueUrl = data['QueueUrl']
  var count = 0

  process.stdin.resume()
  process.stdin.setEncoding('utf8')

  var data = ''
  process.stdin.on('data', function(chunk) {
    data += chunk
  })

  process.stdin.on('end', function() {
    var entries = data.split('\n').map(function (line, index) {
      return {'Id': String(index), 'MessageBody': line}
    })

    var count = 0
    sendBatch()

    function sendBatch() {
      var batch = entries.splice(0, 10)
      if (batch.length > 0) {
        count += batch.length
        sqs.sendMessageBatch({'QueueUrl': queueUrl, 'Entries': batch}, function (err, data) {
          if (err) {
            console.error(err.stack)
            process.exit(1)
          }
          console.log(count + ' messages sent')
          sendBatch()
        })
      } else {
        console.log('All Done')
      }
    }
  })

})
