Draccus
=======

A tool for stashing messages queued up in Amazon's SQS.

Use it as a disaster recovery mechanism to drain a queue, or use it to guarantee
delivery of transaction logs to persistent storage.

SQS Messages will only be deleted once they have been permanently stored, thus
certain classed of errors may cause messages to be stored twice. This is by
design to avoid the possibility of dropping messages.  If you care about
uniqueness make sure there is an identifier in the message body that can be used
by any future batch processing jobs that might run over the messages.

Install
-------

```
$ npm install draccus
$ npm test draccus
```


Usage
-----

    dracus.js --aws_config path/to/config --queue_name some_queue_name

### CLI Flags

- `--aws_config` : Path to a JSON file where configuration can be loaded from.
- `--access_key_id` : Your AWS access key ID, if not specified in aws_config
- `--secret_access_key` : Your AWS access key ID, if not specified in aws_config
- `--queue_name` : The name of the queue to read messages from.
- `--flush_frequency` : How often to flush messages to the store. Default: 60s.
- `--s3_bucket` : Name of an S3 bucket where messages should be written
- `--out_dir` : A local directory to write messages to (e.g. for dev)
- `--stdout` : Simply logs messages to the console
- `--daemon` : Whether the process should stay running once the queue is empty,
  and wait for further messages.
- `filename_pattern` : How to generate filenames, uses [momentjs](http://momentjs.com/docs/#/displaying/)
  for string formatting.  Default: "X" for unix timestamp.


### --aws_config

The above flags can be specified via a JSON file, along with additional AWS configuration options as
described in the
[SDK Documentation](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/SQS_20121105.html#constructor-property).

    {
      "accessKeyId": "YOURACCESSKEY",
      "secretAccessKey": "yoursecretkey",
      "sslEnabled": true,
      "region": "us-west-2",
      "queueName": "my_awesome_queue",
      "s3Bucket": "drained_queue"
    }

The JSON flags are camel case equivalents of the CLI flags.
