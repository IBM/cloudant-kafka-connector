# 0.200.0 (2022-11-01)

- [FIXED] README and documentation have been extensively rewritten.
- [BREAKING CHANGE] Rename source connector. Properties files should be updated to
  use `connector.class=com.ibm.cloud.cloudant.kafka.SinkConnector`.
- [BREAKING CHANGE] Rename sink connector. Properties files should be updated to
  use `connector.class=com.ibm.cloud.cloudant.kafka.SinkConnector`.
- [BREAKING CHANGE] Configuration parameters have changed for url, database, authentication, and last change sequence.
  See README for details.
- [BREAKING CHANGE] Source connector flatten, schema generation and omit design documents options have been replaced by
  message transforms. See README for details.
- [BREAKING CHANGE] Source connector now emits `java.util.Map` (not `String`) record values by default. See README for
  details.
- [BREAKING CHANGE] Source connector now emits `org.apache.kafka.connect.source.SourceRecord` (not `String`) record keys
  by default. See README for details.
- [BREAKING CHANGE] Source connector now emits tombstone events for deleted documents.
  See [single message transforms](README.md#single-message-transforms) section in README for details.
- [BREAKING CHANGE] Converter support for sink connector has changed. See README for details.
- [BREAKING CHANGE] Preserve `_rev` field message values in sink connector.
  See [sink connector config](README.md#converter-configuration-sink-connector) section in README for more details.
- [BREAKING CHANGE] Semantics of `batch.size` configuration parameter changed: for sink connector this value no longer
  affects when `flush()` is called.
- [BREAKING CHANGE] Sink connector will correctly honour `errors.tolerance`, `errors.log.enable`,
  and `errors.deadletterqueue.topic.name` configuration parameters.
  See [the sample sink properties file](docs/connect-cloudant-sink-example.properties) for a recommended example of how
  to configure these to continue processing when non-fatal errors occur.
- [BREAKING CHANGE] Rename jar from `kafka-connect-cloudant` to `cloudant-kafka-connector`.
- [BREAKING CHANGE] Publish releases to https://github.com/IBM/cloudant-kafka-connector/releases.
- [UPGRADED] Connector now supports all authentication types via the `cloudant.auth.type` configuration parameter. When
  using an authentication type of "iam", the API key is configured via the `cloudant.apikey` configuration parameter.
- [UPGRADED] Upgraded Gradle distribution from 4.5.1 to 7.4
- [UPGRADED] Upgraded Kafka Connect API to 3.2.1.
- [UPGRADED] Refactored to use the new `cloudant-java-sdk` library.

# 0.100.2-kafka-1.0.0 (2018-04-13)

- [FIXED] Issue where multiple source connector configurations overlapped causing incorrect message
 publication or duplication.

# 0.100.1-kafka-1.0.0 (2018-03-19)

- [FIXED] Corrected version number reported by the source and sink connectors and tasks.

# 0.100.0-kafka-1.0.0 (2018-03-16)

- [NEW] Configuration option `cloudant.value.schema.struct` to send the Cloudant document message
 with a `org.apache.kafka.connect.data.Struct` value schema instead of as a `String`.
- [NEW] Configuration option `cloudant.value.schema.struct.flatten` to flatten nested objects and
 arrays when using `cloudant.value.schema.struct=true`.
 - [NEW] Configuration option `cloudant.omit.design.docs` to suppress message production for design
 documents.
- [BREAKING CHANGE] Changed build tooling to Gradle.
- [IMPROVED] Added `kafka-connect-cloudant` User-Agent header to requests.
- [UPGRADED] Upgraded for Kafka 1.0.0
- [NOTE] Version number format. The kafka-connect-cloudant version is no longer identical
 to an Apache Kafka version. The Apache Kafka version is appended to the kafka-cloudant-connect
 version. In this way fixes can be released into kafka-cloudant-connect independently of Apache Kafka
 version changes, but the compatible Apache Kafka API level is still recorded as part of the
 kafka-connect-cloudant version string e.g. `kafka-connect-cloudant-x.y.z-kafka-m.n.p`.

# 0.10.2.0 (2017-03-16)

- [UPGRADED] Upgraded for Kafka 0.10.2.0

# 0.10.1 (2017-02-08)

- Initial release for Kafka 0.10.1
