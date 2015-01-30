// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Manages options and flags.
 */

var AWS = require('aws-sdk')
var flags = require('flags')
var fs = require('fs')
var path = require('path')

flags.defineString('options_file', '', 'JSON file containing configuration options.')

flags.defineString('access_key_id', '', 'Your AWS access key ID.')
flags.defineString('secret_access_key', '', 'Your AWS secret access key.')
flags.defineString('metadata_service_host', '', 'IAM metadata service (default: 169.254.169.254).')
flags.defineString('region', '', 'The AWS Region where the queue resides, overrides config file.')
flags.defineString('queue_name', '', 'The name of the SQS queue to receive messages from.')
flags.defineBoolean('sslEnabled', true, 'Whether SSL should be used when communicating with AWS')

flags.defineString('udp_host', '', 'Host to which you want to send a udp copy of the messages')
flags.defineNumber('udp_port', null, 'Port to which you want to send a udp copy of the messages')


flags.defineString('s3_bucket', '', 'The S3 bucket where messages should be written')
flags.defineString('out_dir', '', 'Local directory where files will be written')
flags.defineBoolean('stdout', false, 'Write messages to the console')
flags.defineBoolean('log_raw_message', false, 'Whether to log the raw SQS message instead of the body')

flags.defineString('filename_pattern', '', 'How to generate filenames. Uses momentjs date ' +
    'formatting options.  Default: X')

flags.defineNumber('flush_frequency', 0, 'How often the store should flush messages, in seconds')
flags.defineBoolean('daemon', false, 'Whether the process should stay running once the queue ' +
    'is empty, and wait for further messages.')
flags.defineString('log_file', '', 'A file to write logs to.')

flags.parse()

module.exports = createOptions()


/**
 * Creates an options object to pass to the AWS-SDK, if specified a config file will be used.
 * Flags will override values in the config.
 * @return {Object}
 */
function createOptions() {

  var options

  if (flags.get('options_file')) {
    var optionsFile = path.resolve(process.cwd(), flags.get('options_file'))
    try {
      options = JSON.parse(fs.readFileSync(optionsFile, 'utf8'))
    } catch (e) {
      console.error('Unable to load options from "' + optionsFile + '": ' + e.message)
      process.exit(1)
    }
  } else {
    options = {}
  }

  // Allow CLI overrides of AWS config options.
  if (flags.get('region')) options.region = flags.get('region')
  if (flags.get('access_key_id')) options.accessKeyId = flags.get('access_key_id')
  if (flags.get('secret_access_key')) options.secretAccessKey = flags.get('secret_access_key')
  if (flags.get('metadata_service_host')) options.metadataServiceHost = flags.get('metadata_service_host')
  if (flags.get('sslEnabled')) options.sslEnabled = flags.get('sslEnabled')

  // Non AWS config options, but included for convenience, will be removed
  // before being passed to the AWS SDK.
  if (flags.get('queue_name')) options.queueName = flags.get('queue_name')
  if (flags.get('s3_bucket')) options.s3Bucket = flags.get('s3_bucket')
  if (flags.get('out_dir')) options.outDir = flags.get('out_dir')

  options.flushFrequency = flags.get('flush_frequency') || options.flushFrequency || 60
  options.filenamePattern = flags.get('filename_pattern') || options.filenamePattern || 'X'
  options.logFile = flags.get('log_file') || options.logFile || ''

  options.logRawMessage = flags.get('log_raw_message') || options.logRawMessage || false
  options.daemon = flags.get('daemon')
  options.stdout = flags.get('stdout')

  options.udp_host = flags.get('udp_host')
  options.udp_port = flags.get('udp_port')

  // Fallback to IAM based credentials.
  if (!options.accessKeyId) {
    options.credentials = new AWS.EC2MetadataCredentials({
      host: options.metadataServiceHost || '169.254.169.254'
    })
  }

  // Force API version.
  options.apiVersion = '2012-11-05'

  return options
}
