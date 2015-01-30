// Copyright 2013 The Obvious Corporation.

/**
 * @fileoverview Sub-class of MessageStore that writes files to S3.
 */

var MessageStore = require('./MessageStore')
var logg = require('logg')
var util = require('util')


/**
 * @param {SqsSink} sink
 * @param {AWS.S3} s3
 * @param {string} filenamePattern momentjs compatible string for generating filenames
 * @param {string} bucket
 * @param {number} flushFreqSec
 * @constructor
 */
function S3Store(sink, s3, filenamePattern, bucket, flushFreqSec, udpPort, udpHost) {
  MessageStore.call(this, sink, '', filenamePattern, flushFreqSec, udpPort, udpHost)
  this._s3 = s3
  this._bucket = bucket
}
util.inherits(S3Store, MessageStore)
module.exports = S3Store


S3Store.prototype.logger = logg.getLogger('S3Store')


/**
 * Verifies the bucket exists and can be written to.
 * @param {function (Error, boolean)} callback Whether bucket can be written to.
 */
S3Store.prototype.verifyBucket = function (callback) {
  this._s3.headBucket({'Bucket': this._bucket}, function (err, data) {
    callback(err || null, !!data)
  })
}


/** @override */
S3Store.prototype.write = function (data, callback) {
  var filename = this.newFilename()
  var identifier = 'Bucket: ' + this._bucket + ', File: ' + filename
  this._s3.putObject({
    'ACL': 'private', // Or bucket-owner-full-control ??
    'Key': filename,
    'Bucket': this._bucket,
    'ContentType': 'text/plain',
    'Body': new Buffer(data, 'utf8')
  }, function (err, data) {
    callback(err || null, identifier)
  })
}
