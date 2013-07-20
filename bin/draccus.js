#!/usr/bin/env node

// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview External interface to draccus program.
 *
 * Usage:
 *  ./draccus.js --daemon --aws_config config.json
 *
 */

var AWS = require('aws-sdk')
var flags = require('flags')
var fs = require('fs')
var path = require('path')
var MessageStore = require('../lib/MessageStore')
var S3Store = require('../lib/S3Store')
var StdoutStore = require('../lib/StdoutStore')
var SqsSink = require('../lib/SqsSink')

flags.defineString('aws_config', '', 'JSON file containing AWS SQS configuration options.')

flags.defineString('access_key_id', '', 'Your AWS access key ID.')
flags.defineString('secret_access_key', '', 'Your AWS secret access key.')
flags.defineString('region', '', 'The AWS Region where the queue resides, overrides config file.')
flags.defineString('queue_name', '', 'The name of the SQS queue to receive messages from.')

flags.defineString('s3_bucket', '', 'The S3 bucket where messages should be written')
flags.defineString('out_dir', '', 'Local directory where files will be written')
flags.defineBoolean('stdout', false, 'Write messages to the console')

flags.defineNumber('flush_frequency', 60, 'How often the store should flush messages, in seconds')
flags.defineBoolean('daemon', false, 'Whether the process should stay running once the queue ' +
    'is empty, and wait for further messages.')

flags.parse()

var options = createOptions()


// Load SQS and get the queue name.
var sqs = new AWS.SQS(getAwsOptions(options))
sqs.getQueueUrl({'QueueName': options.queueName}, function (err, data) {
  if (err) exit(1, 'Unable to resolve queue "' + options.queueName + '": ' + err.message)

  // The SDK swallows errors in the callback. So complete the set up on next tick.
  process.nextTick(function () {
    var store

    if (options.s3Bucket) {
      var s3 = new AWS.S3(getAwsOptions(options))
      store = new S3Store(s3, options.s3Bucket, options.flushFrequency)
      store.verifyBucket(function (err, writable) {
        if (!writable) {
          exit(1, 'S3 Bucket "' + options.s3Bucket + '"" not writable, ' + err.statusCode + ' ' + err.name)
        }
      })

    } else if (options.outDir) {
      var outDir = path.join(process.cwd(), options.outDir)
      store = new MessageStore(outDir, 1000 * options.flushFrequency)

    } else if (flags.get('stdout')) {
      store = new StdoutStore()

    } else {
      exit(1, 'You must specify one of --stdout, --out_dir, or --s3_bucket')
    }

    new SqsSink(sqs)
      .setQueueUrl(data['QueueUrl'])
      .setStopWhenEmpty(!flags.get('daemon'))
      .setVisibilityTimeSec(options.flushFrequency * 1.25) // Extra 25% leeway to ack messages.
      .setStore(store)
      .receive()
  })
})


/**
 * Creates an options object to pass to the AWS-SDK, if specified a config file will be used.
 * Flags will override values in the config.
 * @return {Object}
 */
function createOptions() {
  // TODO(dan): This configuration code is pretty ugly.  Figure out a better way of allowing
  // a combination of static configuration and flags.

  var options

  if (flags.get('aws_config')) {
    var optionsFile = path.join(process.cwd(), flags.get('aws_config'))
    try {
      options = JSON.parse(fs.readFileSync(optionsFile, 'utf8'))
    } catch (e) {
      exit(1, 'Unable to load SQS options from "' + optionsFile + '": ' + e.message)
    }
  } else {
    options = {}
  }

  // Allow CLI overrides of AWS config options.
  if (flags.get('region')) options.region = flags.get('region')
  if (flags.get('access_key_id')) options.accessKeyId = flags.get('access_key_id')
  if (flags.get('secret_access_key')) options.secretAccessKey = flags.get('secret_access_key')

  // Non AWS config options, but included for convenience, will be removed
  // before being passed to the AWS SDK.
  if (flags.get('queue_name')) options.queueName = flags.get('queue_name')
  if (flags.get('s3_bucket')) options.s3Bucket = flags.get('s3_bucket')
  if (flags.get('out_dir')) options.outDir = flags.get('out_dir')

  options.flushFrequency = flags.get('flush_frequency')

  // Force API version.
  options.apiVersion = '2012-11-05'

  return options
}


function getAwsOptions(options) {
  var o = shallowClone(options)
  delete o.queueName
  delete o.s3Bucket
  delete o.outDir
  delete o.flushFrequency
  return o
}


function shallowClone(obj) {
  var o = {}
  for (var k in obj) {
    o[k] = obj[k]
  }
  return o
}

/**
 * Exits the process with a status code, logging an error message.
 * @param {number} code
 * @param {string} message
 */
function exit(code, message) {
  console.error(message)
  process.exit(code)
}
