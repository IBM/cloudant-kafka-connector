# Unreleased

- [NEW] Configuration option `cloudant.value.schema.struct` to send the Cloudant document message
 with a `org.apache.kafka.connect.data.Struct` value schema instead of as a `String`.
- [BREAKING CHANGE] Changed build tooling to Gradle.
- [NOTE] Version number format. The kafka-connect-cloudant version is no longer identical
 to an Apache Kafka version. The Apache Kafka version is appended to the kafka-cloudant-connect
 version. In this way fixes can be released into kafka-cloudant-connect independently of Apache Kafka
 version changes, but the compatible Apache Kafka API level is still recorded as part of the
 kafka-connect-cloudant version string e.g. `kafka-connect-cloudant-x.y.z-kafka-m.n.p`.

# 0.10.2.0 (2017-03-16)

- [UPGRADED] Upgraded for Kafka 0.10.2.0

# 0.10.1 (2017-02-08)

- Initial release for Kafka 0.10.1
