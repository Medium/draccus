Draccus
=======

[![Build Status](https://secure.travis-ci.org/Obvious/draccus.png)](http://travis-ci.org/Obvious/draccus)


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

    dracus.js --options_file path/to/config --queue_name some_queue_name

### CLI Flags

- `--options_file` : Path to a JSON file where options can be loaded from.
- `--access_key_id` : AWS access key ID.
- `--secret_access_key` : AWS secret access key.
- `--metadata_service_host` : Hostname to use when looking up credentials via IAM (see below).
- `--queue_name` : The name of the queue to read messages from.
- `--flush_frequency` : How often to flush messages to the store. Default: 60s.
- `--s3_bucket` : Name of an S3 bucket where messages should be written.
- `--out_dir` : A local directory to write messages to (e.g. for dev).
- `--stdout` : Simply logs messages to the console (warning: will still erase the queue contents).
- `--daemon` : Whether the process should stay running once the queue is empty,
  and wait for further messages.
- `--filename_pattern` : How to generate filenames, uses [momentjs](http://momentjs.com/docs/#/displaying/).
  for string formatting, in addition replacing `PID` with the process id.
  Default: `X` for unix timestamp.
- `--log_file` : The path of a file to write logs too.
- `--log_raw_message` : Specifies that the raw SQS message should be stored as JSON, instead of the message body.


### --options_file

The above flags can be specified via a JSON file, along with additional AWS configuration options as
described in the
[SDK Documentation](http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/SQS_20121105.html#constructor-property).

    {
      "accessKeyId": "YOURACCESSKEY",
      "secretAccessKey": "yoursecretkey",
      "sslEnabled": true,
      "region": "us-west-2",
      "queueName": "my_awesome_queue",
      "s3Bucket": "drained_queue",
      "filenamePattern": "YYYY/MM/DD/HH-X-PID"
    }

The JSON flags are camel case equivalents of the CLI flags.


AWS Credentials
---------------

When running outside of EC2 you will need to pass in a valid access key and secret key.  However, if
you are running within EC2 and you omit the access key draccus will fall back to trying to use IAM
based credentials, using the instance's role.
See [here](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AESDG-chapter-instancedata.html)
for more details.


Throughput
----------

Throughput should mostly be limited by network latency and SQS response time.
From [SQS documentation](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/throughput.html)
the theoretical limit per instance should be 500 messages per second.

In a test on an m1.small it took 40.7s to handle 10,000 messages. That is:
receive, write to S3, and delete the message.  The test flushed to S3 every 10s,
resulting in 3x 100k files.

Test set up:

    draccus-fill-queue --options_file aws.config --batches 1000
    time draccus --options_file aws.config --flush_frequency 10


(If you use multiple workers remember to specify a different filename pattern or
include `PID` in the filename pattern.)


Contributing
------------

Questions, comments, bug reports, and pull requests are all welcome.  Submit them at
[the project on GitHub](https://github.com/Obvious/draccus/).  If you haven't contributed to an
[Obvious](http://github.com/Obvious/) project before please head over to the
[Open Source Project](https://github.com/Obvious/open-source#note-to-external-contributors) and fill
out an OCLA (it should be pretty painless).

Bug reports that include steps-to-reproduce (including code) are the
best. Even better, make them in the form of pull requests.

Author
------

[Dan Pupius](https://github.com/dpup)
([personal website](http://pupius.co.uk/)), supported by
[The Obvious Corporation](http://obvious.com/).

License
-------

Copyright 2013 [The Obvious Corporation](http://obvious.com/).

Licensed under the Apache License, Version 2.0.
See the top-level file `LICENSE.txt` and
(http://www.apache.org/licenses/LICENSE-2.0).
