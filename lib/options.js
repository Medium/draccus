// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Manages options and flags.
 */

var flags = require('flags')
var fs = require('fs')
var path = require('path')

flags.defineString('aws_config', '', 'JSON file containing AWS SQS configuration options.')

flags.defineString('access_key_id', '', 'Your AWS access key ID.')
flags.defineString('secret_access_key', '', 'Your AWS secret access key.')
flags.defineString('region', '', 'The AWS Region where the queue resides, overrides config file.')
flags.defineString('queue_name', '', 'The name of the SQS queue to receive messages from.')

flags.defineString('s3_bucket', '', 'The S3 bucket where messages should be written')
flags.defineString('out_dir', '', 'Local directory where files will be written')
flags.defineBoolean('stdout', false, 'Write messages to the console')

flags.defineString('filename_pattern', '', 'How to generate filenames. Uses momentjs date ' +
    'formatting options.  Default: X')

flags.defineNumber('flush_frequency', 0, 'How often the store should flush messages, in seconds')
flags.defineBoolean('daemon', false, 'Whether the process should stay running once the queue ' +
    'is empty, and wait for further messages.')

flags.parse()


module.exports = createOptions()



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
    var optionsFile = path.resolve(process.cwd(), flags.get('aws_config'))
    try {
      options = JSON.parse(fs.readFileSync(optionsFile, 'utf8'))
    } catch (e) {
      console.error('Unable to load SQS options from "' + optionsFile + '": ' + e.message)
      process.exit(1)
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

  options.flushFrequency = flags.get('flush_frequency') || options.flushFrequency || 60
  options.filenamePattern = flags.get('filename_pattern') || options.filenamePattern || 'X'

  options.daemon = flags.get('daemon')
  options.stdout = flags.get('stdout')

  // Force API version.
  options.apiVersion = '2012-11-05'

  // Get a clean object that contains only what should be passed to the SDK
  // constructors.  Each call produces a shallow clone to avoid conflicts by
  // multiple users.
  options.getAwsOptions = function () {
    var o = shallowClone(options)
    delete o.queueName
    delete o.s3Bucket
    delete o.outDir
    delete o.flushFrequency
    return o
  }

  return options
}


function shallowClone(obj) {
  var o = {}
  for (var k in obj) {
    o[k] = obj[k]
  }
  return o
}
