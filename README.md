# Kafka Connect Cloudant

Kafka Connect Cloudant Connector. This project includes source & sink connectors.

## Release Status

Experimental

## Table of Contents

* Configuration
* Usage
* Building from Source
* Test execution

## Configuration

The Cloudant Kafka connector can be configured in standalone or distributed mode according to the [Kafka Connector documentation](http://docs.confluent.io/3.0.1/connect/userguide.html#configuring-connectors).

Make sure to include the following values for either configuration:

Parameter | Value
---:|:---
key.converter|org.apache.kafka.connect.json.JsonConverter
value.converter|org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable|true
value.converter.schemas.enable|true

Assume these settings in a file `connect-standalone.properties` or `connect-distributed.properties`.

### Cloudant as source

To read from a Cloudant database as source and write documents to a Kafka topic, create a source configuration file 

`connect-cloudant-source.properties`

with the following parameters:

Parameter | Value | Required | Default value
---:|:---|:---|:---
name|cloudant-source|YES|None
connector.class|com.ibm.cloudant.kafka.connect.CloudantSourceConnector|YES|None
tasks.max|5|NO|1
cloudant.db.url|https://\<account\>.cloudant.com/\<database\>|YES|None
cloudant.db.username|\<username\>|YES|None
cloudant.db.password|\<password\>|YES|None
cloudant.db.since|1-g1AAAAETeJzLYWBgYMlgTmGQT0lKzi9..|NO|0
topics|\<topic1\>,\<topic2\>,..|YES|None


### Cloudant as sink

To consume messages from a Kafka topic and save as documents into a Cloudant database, create a sink configuration file

`connect-cloudant-sink.properties`

with the following parameters:

Parameter | Value | Required | Default value
---:|:---|:---|:---
name|cloudant-sink|YES|None
connector.class|com.ibm.cloudant.kafka.connect.CloudantSinkConnector|YES|None
tasks.max|5|NO|1
cloudant.db.url|https://\<account\>.cloudant.com/\<database\>|YES|None
cloudant.db.username|\<username\>|YES|None
cloudant.db.password|\<password\>|YES|None
topics|\<topic1\>,\<topic2\>,..|YES|None

## Usage

Kafka will use the $CLASSPATH to locate available connectors. Make sure to add the connector library to your $CLASSPATH first. 

Connector execution in Kafka is available through scripts in the Kafka install path:

`$kafka_home/bin/connect-standalone.sh` or `$kafka_home/bin/connect-distributed.sh`

Use the appropriate configuration files for standalone or distributed execution with Cloudant as source, as sink, or both.

For example:
- standalone execution with Cloudant as source:

  ```
  $kafka_home/bin/connect-standalone connect-standalone.properties connect-cloudant-source.properties`
  ```

- standalone execution with Cloudant as sink:

  ```
  $kafka_home/bin/connect-standalone connect-standalone.properties connect-cloudant-sink.properties
  ```

- standalone execution with multiple configurations, one using Cloudant as source and one using Cloudant as sink:

  ```
  $kafka_home/bin/connect-standalone connect-standalone.properties connect-cloudant-source.properties connect-cloudant-sink.properties
  ```

Any number of connector configurations can be passed to the executing script. 

INFO level logging is configured by default to the console. To change log levels or settings, work with

`$kafka_home/etc/kafka/connect-log4j.properties`

and add log settings like

`log4j.logger.com.ibm.cloudant.kafka=DEBUG, stdout`

## Building from Source

The project requires Java 8 and Maven 2 to build from source. Execute the following command in the project directory:

```
mvn clean install
```

## Test execution

Junit tests are available in `src/test/java`. To execute, please modify values in `src/test/resources`, including:

- log4j.properties (optional)
- test.properties (required)

The settings in `test.properties` have to include Cloudant database credentials and Kafka topic details as above.

