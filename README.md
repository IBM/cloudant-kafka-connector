# Cloudant Kafka Connector

[![Release](https://img.shields.io/github/v/release/IBM/cloudant-kafka-connector?include_prereleases)](https://github.com/IBM/cloudant-kafka-connector/releases/latest)

This project includes [Apache Kafka](https://kafka.apache.org/) [Connect](https://kafka.apache.org/documentation.html#connect) source and sink connectors for IBM Cloudant.

These connectors can stream events:
- **from** Cloudant (source connector) to Kafka topic(s)
- **to** Cloudant (sink connector) from Kafka topic(s)

_Note:_ the connectors are also compatible with Apache CouchDB.

## Release Status

Experimental

## Usage

**Note**: The below instructions assume an installation of Kafka at `$KAFKA_HOME`.

### Quick Start

1. Download the zip from the [releases page](https://github.com/IBM/cloudant-kafka-connector/releases). The zip file
   contains the plugin jar and the non-Kafka dependencies needed to run.
2. Configure the [Kafka connect plugin path](https://kafka.apache.org/documentation.html#connectconfigs_plugin.path) for
   your Kafka distribution, for example: `plugin.path=/kafka/connect`.
   - This will be configured in either `connect-standalone.properties` or `connect-distributed.properties` in
     the `config` directory of your Kafka installation.
   - If you're not sure which to use, edit `connect-standalone.properties` and follow the standalone execution
     instructions below.
2. Unzip and move to the plugin path configured earlier, for example:
   `unzip cloudant-kafka-connector-x.y.z.zip; mv cloudant-kafka-connector-x.y.z /kafka/connect`.
3. Edit the [source](docs/connect-cloudant-changes-source-example.properties)
   or [sink](docs/connect-cloudant-sink-example.properties) example properties files and save this to the `config`
   directory of your Kafka installation.
4. Start Kafka.
5. Start the connector (see below).

Connector execution in Kafka is available through scripts in the Kafka install path:

`$KAFKA_HOME/bin/connect-standalone.sh` or `$KAFKA_HOME/bin/connect-distributed.sh`

Use the appropriate configuration files for standalone or distributed execution with Cloudant as source, as sink, or both.

For example:
- standalone execution with Cloudant changes feed as source:

  ```
  $KAFKA_HOME/bin/connect-standalone.sh \
  $KAFKA_HOME/config/connect-standalone.properties \
  $KAFKA_HOME/config/connect-cloudant-source.properties
  ```

- standalone execution with Cloudant as sink:

  ```
  $KAFKA_HOME/bin/connect-standalone.sh \
  $KAFKA_HOME/config/connect-standalone.properties \
  $KAFKA_HOME/config/connect-cloudant-sink.properties
  ```

- standalone execution with multiple configurations, one using Cloudant as source and one using Cloudant as sink:

  ```
  $KAFKA_HOME/bin/connect-standalone.sh \
  $KAFKA_HOME/config/connect-standalone.properties \
  $KAFKA_HOME/config/connect-cloudant-source.properties \
  $KAFKA_HOME/config/connect-cloudant-sink.properties
  ```

Any number of connector configurations can be passed to the executing script.


## Configuration

As outlined above, the Cloudant Kafka connector can be configured in standalone or distributed mode according to 
the [Kafka Connector documentation](https://kafka.apache.org/documentation.html#connect_configuring).

The `connect-standalone` or `connect-distributed` configuration files contain default values which are necessary for all connectors, such as:

1. `bootstrap.servers`
2. If using a standalone worker `offset.storage.file.filename`.
3. `offset.flush.interval.ms`

### Connector configuration

The [`cloudant-changes-source-example`](docs/connect-cloudant-changes-source-example.properties) and [`cloudant-sink-example`](docs/connect-cloudant-sink-example.properties) properties files contain the minimum required to get started.
For a full reference explaining all the connector options, see [here (source)](docs/configuration-reference-changes-source.md) and
[here (sink)](docs/configuration-reference-sink.md).

#### Authentication

In order to read from or write to Cloudant, some authentication properties need to be configured. These properties are common to both the source and sink connector, and are detailed in the configuration reference, linked above.

A number of different authentication methods are supported. IBM Cloud IAM-based authentication methods are recommended and the default is to use an IAM API key. See [locating your service credentials](https://cloud.ibm.com/docs/Cloudant?topic=Cloudant-locating-your-service-credentials) for details on how to find your IAM API key.

### Converter Configuration

Also present in the `connect-standalone` or `connect-distributed` configuration files are defaults for key and value conversion, which are as follows:
```
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true
```

Depending on your needs, you may need to change these converter settings.
For instance, in the sample configuration files, value schemas are disabled on the assumption that users will read and write events which are "raw" JSON and do not have inline schemas.

#### Converter Configuration: source connector

For the source connector:
* Keys are produced as a `org.apache.kafka.connect.data.Struct` containing:
  * `_id`: the original Cloudant document ID
  * `cloudant.db`: the name of the Cloudant database the event originated from
  * `cloudant.url`: the URL of the Cloudant instance the event originated from.
* Values are produced as a (schemaless) `java.util.Map<String, Object>`.
* These types are compatible with the default `org.apache.kafka.connect.json.JsonConverter` and should be compatible with any other converter that can accept a `Struct` or `Map`.
* The `schemas.enable` may be safely used with a `key.converter` if desired.
* The source connector does not generate schemas for the event values by default. To use `schemas.enable` with the `value.converter` consider using a schema registry or the [`MapToStruct` SMT](docs/smt-reference.md#map-to-struct-conversion).

#### Converter Configuration: sink connector

For the sink connector:
* Kafka keys are currently ignored; therefore the key converter settings are not relevant.
* We assume that the values in kafka are serialized JSON objects, and therefore `JsonConverter` is supported. If your values contain a schema (`{"schema": {...}, "payload": {...}}`), then set `value.converter.schemas.enable=true`, otherwise set `value.converter.schemas.enable=false`. Any other converter that converts the message values into `org.apache.kafka.connect.data.Struct` or `java.util.Map` types should also work. However, it must be noted that the subsequent serialization of `Map` or `Struct` values to JSON documents in the sink may not match expectations if a schema has not been provided.
* Inserting only a single revision of any `_id` is currently supported.  This means it cannot update or delete documents.
* The `_rev` field in event values are preserved.  To remove `_rev` during data flow, use the [`ReplaceField` SMT](docs/smt-reference.md#removing-_rev-field).

**Note:** The ID of each document written to Cloudant by the sink connector can be configured as follows:

* From the value of the `cloudant_doc_id` header on the even, which will overwrite the `_id` field if it already exists.
  [The Mapping Document IDs section](docs/smt-reference.md#mapping-document-ids) of the SMT reference shows an example of how to use this header to set the ID based on the event key.
* The value of the `_id` field in the JSON.
* If no other non-null or non-empty value is available the document will be created with a new UUID.

### Single Message Transforms

A number of SMTs (Single Message Transforms) have been provided as part of the library to customize fields or values of events during data flow.

See the [SMT reference](docs/smt-reference.md) for an overview of how to use these and Kafka built-in SMTs for common use cases.

### Logging

INFO level logging is configured by default to the console. To change log levels or settings, work with

`$KAFKA_HOME/config/connect-log4j.properties`

and add log settings like

`log4j.logger.com.ibm.cloud.cloudant.kafka=DEBUG, stdout`

## Release Process

For developers, the release process can be found [here](docs/releasing.md)
