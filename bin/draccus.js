#!/usr/bin/env node

// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview External interface to draccus program.
 *
 * Usage:
 *  ./draccus.js --daemon --aws_config config.json
 *
 */

var logg = require('logg')
var fs = require('fs')
var path = require('path')
var AWS = require('aws-sdk')
var MessageStore = require('../lib/MessageStore')
var S3Store = require('../lib/S3Store')
var StdoutStore = require('../lib/StdoutStore')
var SqsSink = require('../lib/SqsSink')

var options = require('../lib/options')


if (options.logFile) {
  var fileName = path.resolve(process.cwd(), options.logFile)
  var writeStream = fs.createWriteStream(fileName, {flags: 'a', encoding: 'utf8', mode: 0666})
  logg.registerWatcher(function (logRecord) {
    writeStream.write(
        logRecord.date.toISOString() + ' [' +
        logg.Level.toString(logRecord.level) + '] ' +
        (logRecord.name ? '(' + logRecord.name + ') ' : '') +
        logRecord.getFormattedMessage() + '\n')
  })
}


// Load SQS and get the queue name.
var sqs = new AWS.SQS(options)
sqs.getQueueUrl({'QueueName': options.queueName}, function (err, data) {
  if (err) exit(1, 'Unable to resolve queue "' + options.queueName + '": ' + err.message)

  // The SDK swallows errors in the callback. So complete the set up on next tick.
  process.nextTick(function () {

    // Create a sink for receiving messages from SQS.
    var sink = new SqsSink(sqs, options.logRawMessage)
      .setQueueUrl(data['QueueUrl'])
      .setStopWhenEmpty(!options.daemon)
      .setVisibilityTimeSec(options.flushFrequency * 1.25) // Extra 25% leeway to ack messages.

    if (options.s3Bucket) {
      var s3 = new AWS.S3(options)
      new S3Store(sink, s3, options.filenamePattern, options.s3Bucket, options.flushFrequency)
          .verifyBucket(function (err, writable) {
            if (!writable) exit(1, 'S3 Bucket "' + options.s3Bucket + '" not writable, ' + err.statusCode + ' ' + err.name)
            else sink.startReceiving()
          })

    } else if (options.outDir) {
      var outDir = path.resolve(process.cwd(), options.outDir)
      new MessageStore(sink, outDir, options.filenamePattern, options.flushFrequency)
      sink.startReceiving()

    } else if (options.stdout) {
      new StdoutStore(sink)
      sink.startReceiving()

    } else {
      exit(1, 'You must specify one of --stdout, --out_dir, or --s3_bucket')
    }
  })
})


/**
 * Exits the process with a status code, logging an error message.
 * @param {number} code
 * @param {string} message
 */
function exit(code, message) {
  logg.getLogger('').error(message)
  process.exit(code)
}
