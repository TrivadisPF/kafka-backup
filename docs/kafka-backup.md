# Kafka Backup & Restore

## Kafka Backup connector guide

An Apache Kafka connect sink connector for doing backups of Kafka topics to Amazon S3. For more information about configuring connectors in general see the official Confluent documentation.

### Sink Connector Configuration Options

To use this connector, specify the name of the connector class in the connector.class configuration property.

`connector.class=ch.tbd.kafka.backuprestore.backup.kafkaconnect.BackupSinkConnector`

Connector-specific configuration properties are described below

### Connector

`topics`

A list of Kafka topics to backup   

  * Type: list
  * Importance: high
  
`flush.size`

Number of records written to store before invoking file commits. 

  * Type: list 
  * Importance: high

`retry.backoff.ms`

The retry backoff in milliseconds. This config is used to notify Connect to retry delivering a message batch or performing recovery in case of transient exceptions.

  * Type: long
  * Default: 5000
  * Importance: low

 
`rotate.schedule.interval.ms`

The time interval in milliseconds to periodically invoke file commits. This configuration ensures that file commits are invoked every configured interval. Time of commit will be adjusted to 00:00 of selected timezone. Commit will be performed at scheduled time regardless previous commit time or number of messages. This configuration is useful when you have to commit your data based on current server time, like at the beginning of every hour. The default value -1 means that this feature is disabled.

  * Type: long
  * Default: -1 (feature is disabled)
  * Importance: low  

`rotate.interval.ms`

The time interval in milliseconds to invoke file commits. This configuration ensures that file commits are invoked every configured interval. This configuration is useful when data ingestion rate is low and the connector didn't write enough messages to commit files. The default value -1 means that this feature is disabled. 

  * Type: long
  * Default: -1 (feature is disabled)
  * Importance: low

-----

### S3

`s3.bucket.name`

The name of the S3 Bucket to write the backup data to

  * Type: string 
  * Importance: high
 
`s3.profile.name`

The profile to use in the Amazon configuration 

  * Type: string 
  * Importance: high

`s3.ssea.name` 

The S3 Server Side Encryption Algorithm

  * Type: string 
  * Importance: low
  
`s3.sse.customer.key`

The S3 Server Side Encryption Customer-Provided Key (SSE-C). 

  * Type: password 
  * Importance: low

`s3.sse.kms.key.id`

The name of the AWS Key Management Service (AWS-KMS) key to be used for server side encryption of the S3 objects. 

  * Type: string 
  * Importance: low

`s3.acl.canned`

An S3 canned ACL header value to apply when writing objects 

  * Type: string
  * Importance: low
  
`s3.proxy.url`

S3 Proxy settings encoded in URL syntax. This property is meant to be used only if you need to access S3 through a proxy 

  * Type: string
  * Importance: low
 
`s3.proxy.user`

S3 Proxy User. This property is meant to be used only if you need to access S3 through a proxy

  * Type: string
  * Importance: low

`s3.proxy.password`

S3 Proxy Password. This property is meant to be used only if you need to access S3 through a proxy

  * Type: password
  * Importance: low
 
`s3.region`

The AWS region to be used the connector

  * Type: string
  * Default: The default region that new customers in the US are encouraged to use when using AWS services for the first time.
  * Importance: medium
  
`s3.service.endpoint`

The Service endpoint to be used for requests. This is not needed when working with Amazon S3 but might be necessary when working with an S3 compliant object store.

  * Type: string
  * Importance: medium
  
`s3.path.style.access`

Enable path style access for all requests. This can be left to default for Amazon S3, but might be needed when working with S3 compliant object stores. 

  * Type: boolean
  * Default: false
  * Importance: medium

`aws.signer.override`

AWS Client Signer Override. This can be left to default for Amazon S3, but might be needed when working with S3 compliant object stores.

  * Tyye: string
  * Importance: medium

`s3.wan.mode`

Configures the client to use S3 accelerate endpoint for all requests. A bucket by default cannot be accessed in accelerate mode. If you wish to do so, you need to enable the accelerate configuration for the bucket in advance. 

  * Type: boolean 
  * Default: false
  * Importance: medium

`s3.retry.backoff.ms`

How long to wait in milliseconds before attempting the first retry of a failed S3 request. Upon a failure, this connector may wait up to twice as long as the previous wait, up to the maximum number of retries. This avoids retrying in a tight loop under failure scenarios.

  * Type: int
  * Default: 200
  * Valid Values: [0,...]
  * Importance: medium

`s3.part.retries`

Maximum number of retry attempts for failed requests. Zero means no retries. The actual number of attempts is determined by the S3 client based on multiple factors, including, but not limited to - the value of this parameter, type of exception occurred, throttling settings of the underlying S3 client, etc.

  * Type: int
  * Default: 3
  * Valid Values: [0,...]
  * Importance: medium

`s3.http.send.expect.continue`

Enable or disable use of the HTTP/1.1 handshake using EXPECT: 100-CONTINUE during multi-part upload. If true, the client will wait for a 100 (CONTINUE) response before sending the request body. Else, the client uploads the entire request body without checking if the server is willing to accept the request.     

  * Type: boolean
  * Default: true
  * Importance: low

`s3.part.size`

The Part Size in S3 Multi-part Uploads.

  * Type: int
  * Default: 26214400
  * Valid Values: [5242880,...,2147483647]
  * Importance: low

`s3.compression.type`

Compression type for file written to S3. Applied when using JsonFormat or ByteArrayFormat. Available values: none, gzip.

  * Type: string
  * Default: none
  * Valid Values: [none, gzip]
  * Importance: low

`s3.retention.policy.days`

The retention in days. It will be used to set the rules on S3 in order to clean the data inside the bucket. Default is 1 day.

  * Type: int
  * Default: 1
  * Valid Values: [1,...]
  * Importance: low


`filename.offset.zero.pad.width`

Width to zero pad offsets in store's filenames if offsets are too short in order to provide fixed width filenames that can be ordered by simple lexicographic sorting.

  * Type: int
  * Default: 10
  * Valid Values: [0,...]
  * Importance: low


-------

### Next

  * [Installation Guide](install.md)
  * The Kafka Backup connector guide
  * The Kafka Restore connector guide
  * A docker end-to-end example
  * Changelog
