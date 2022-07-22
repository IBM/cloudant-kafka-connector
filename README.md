# Cloudant Kafka Connector

[![Maven Central](https://img.shields.io/maven-central/v/com.cloudant/kafka-connect-cloudant.svg)](http://search.maven.org/#search|ga|1|g:"com.cloudant"%20AND%20a:"kafka-connect-cloudant")

This project includes Apache Kafka Connect source & sink connectors for IBM Cloudant.

## Pre-release

**Note**: this README file is for a pre-release version of the
connector. This means it refers to configuration options and features
which are different to the currently released version. For information
about the currently released version, please see the [README
here](https://github.com/IBM/cloudant-kafka-connector/blob/0.100.2-kafka-1.0.0/README.md).

## Release Status

Experimental

## Table of Contents

* Configuration
* Usage

## Configuration

The Cloudant Kafka connector can be configured in standalone or distributed mode according to 
the [Kafka Connector documentation](http://docs.confluent.io/3.0.1/connect/userguide.html#configuring-connectors). At a minimum it is necessary to configure:

1. `bootstrap.servers`
2. If using a standalone worker `offset.storage.file.filename`.

### Converter configuration

The kafka distribution defaults are usually as follows:
```
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true
```

#### Converter configuration: sink connector

For the sink connector:
1. Kafka keys are currently ignored; therefore the key converter settings are not relevant.
1. We assume that the values in kafka are serialized JSON objects, and therefore `JsonConverter` is supported. If your values contain a schema (`{"schema": {...}, "payload": {...}}`), then set `value.converter.schemas.enable=true`, otherwise set `value.converter.schemas.enable=false`. Any other converter that converts the message values into `org.apache.kafka.connect.data.Struct` or `java.util.Map` types should also work. However, it must be noted that the subsequent serialization of `Map` or `Struct` values to JSON documents in the sink may not match expectations if a schema has not been provided.
1. Inserting only a single revision of any `_id` is currently supported.  This means it cannot update or delete documents.
1. The `_rev` field in event values are preserved.  To remove `rev` during data flow, use the `ReplaceField` Single Message Transforms (SMT).
Example configuration:
    ```
    transforms=ReplaceField
    transforms.ReplaceField.type=org.apache.kafka.connect.transforms.ReplaceField$Value 
    transforms.ReplaceField.exclude=_rev
    ```
    See the [Kafka Connect transforms](https://kafka.apache.org/31/documentation.html#connect_transforms) documentation for more details.

**Note:** The ID of each document written to Cloudant by the sink connector can be configured as follows:

1. From the value of the `cloudant_doc_id` header on the event.  The value passed to this header must be a string and the `header.converter=org.apache.kafka.connect.storage.StringConverter` config is required.  This will overwrite the `_id` field if it already exists.
1. The value of the `_id` field in the JSON
1. If no other non-null or non-empty value is available the document will be created with a new UUID.

#### Single Message Transforms

Single Message Transforms, or SMTs, can be used to customize fields or values of events during data flow.  The examples below will explore modifying fields for events flowing from the Kafka topic to a Cloudant database using the sink connector.

1. If the record value contains an existing field, not called `_id`, that is suitable to use as the Cloudant document ID, then you can use the `RenameField` transform.
    ```
    transforms=RenameField
    transforms.RenameField.type=org.apache.kafka.connect.transforms.ReplaceField$Value 
    transforms.RenameField.renames=name:_id
    ```
1. If you have `_id` fields and would prefer to have Cloudant generate a UUID for the document ID, use the `ReplaceField` transform to exclude the existing `_id` field:
    ```
    transforms=ReplaceField
    transforms.ReplaceField.type=org.apache.kafka.connect.transforms.ReplaceField$Value 
    transforms.ReplaceField.exclude=_id
    ```

1. If you have events where there is no value e.g. tombstones (and don't want the Cloudant sink connector to generate an empty doc with a generated ID) then 
you'll need to use a `dropNullRecords` transform and predicate to filter out and remove these tombstone events:  
    ```
    transforms=dropNullRecords
    transforms.dropNullRecords.type=org.apache.kafka.connect.transforms.Filter
    transforms.dropNullRecords.predicate=isNullRecord

    predicates=isNullRecord
    predicates.isNullRecord.type=org.apache.kafka.connect.transforms.predicates.RecordIsTombstone
    ```

1. If you want to use the event key or another custom value as the document ID then use the `cloudant_doc_id` custom header.
   The value set in this custom header will be added to the `_id` field.  If the `_id` field already exists then it will be overwritten
   with the value in this header.
   You can use the `HeaderFrom` SMT to move or copy a key to the custom header. The example config below adds the transform to move 
   the `docid` event key to the `cloudant_doc_id` custom header and sets the header converter to string:
   ```
   transforms=moveFieldsToHeaders
   transforms.moveFieldsToHeaders.type=org.apache.kafka.connect.transforms.HeaderFrom$Key
   transforms.moveFieldsToHeaders.fields=docid
   transforms.moveFieldsToHeaders.headers=cloudant_doc_id
   transforms.moveFieldsToHeaders.operation=move
   
   header.converter=org.apache.kafka.connect.storage.StringConverter
   ```

   **Note**: The `header.converter` is required to be set to `StringConverter` since the document ID field only supports strings.

1. If you have events where the `_id` field is absent or `null` then Cloudant will generate
a document ID. If you don't want this to happen then set an `_id` (see earlier examples).
If you need to filter out those documents or drop `_id` fields when the value is `null` then you'll need to create a custom SMT.

**Note**: For any of the SMTs above, if the field does not exist it will leave the event unmodified and continue processing the next event.

### Authentication

In order to read from or write to Cloudant, some authentication properties need to be configured. These properties are common to both the source and sink connector.

A number of different authentication methods are supported. IAM authentication is the default and recommended method; see [locating your service credentials](https://cloud.ibm.com/docs/Cloudant?topic=Cloudant-locating-your-service-credentials) for details on how to find your IAM API key.

#### cloudant.auth.type

The authentication method (or type). This value is case insensitive.

The default value is `iam`.

Valid values are:

- `iam`
- `couchdb_session`
- `basic`
- `noAuth`
- `bearerToken`
- `container`
- `vpc`.

With the exception of `noAuth`, each of these authentication methods requires one or more additional properties to be set. These are listed below.

#### cloudant.apikey

For use with `iam` authentication.

#### cloudant.username, cloudant.password

For use with `couchdb_session` or `basic` authentication.

#### cloudant.bearer.token

For use with `bearerToken` authentication.

#### cloudant.iam.profile.id 

For use with `container` or `vpc` authentication.

#### cloudant.iam.profile.name

For use with `container` authentication.

#### cloudant.cr.token.filename

For use with `container` authentication.

#### cloudant.iam.profile.crn

For use with `vpc` authentication.

#### cloudant.auth.url, cloudant.scope, cloudant.client.id, cloudant.client.secret

For use with `iam`, `container`, or `vpc` authentication.

### Cloudant as source

In addition to those properties related to authentication, the Cloudant source connector supports the following properties:

Parameter | Value | Required | Default value | Description
---:|:---|:---|:---|:---
name|cloudant-source|YES|None|A unique name to identify the connector with.
connector.class|com.ibm.cloudant.kafka.connect.CloudantSourceConnector|YES|None|The connector class name.
topics|\<topic1\>,\<topic2\>,..|YES|None|A list of topics you want messages to be written to.
cloudant.url|https://\<uuid\>.cloudantnosqldb.appdomain.cloud|YES|None|The Cloudant server to read documents from.
cloudant.db|\<your-db\>|YES|None|The Cloudant database to read documents from.
cloudant.since|1-g1AAAAETeJzLYWBgYMlgTmGQT0lKzi9..|NO|0|The first change sequence to process from the Cloudant database above. 0 will apply all available document changes.
batch.size|400|NO|1000|The batch size used to bulk read from the Cloudant database.
cloudant.omit.design.docs|false|NO|false| Set to true to omit design documents from the messages produced.
cloudant.value.schema.struct|false|NO|false| _EXPERIMENTAL_ Set to true to generate a `org.apache.kafka.connect.data.Schema.Type.STRUCT` schema and send the Cloudant document payload as a `org.apache.kafka.connect.data.Struct` using the schema instead of the default of a string of the JSON document content when using the Cloudant source connector.
cloudant.value.schema.struct.flatten|false|NO|false| _EXPERIMENTAL_ Set to true to flatten nested arrays and objects from the Cloudant document during struct generation. Only used when cloudant.value.schema.struct is true and allows processing of JSON arrays with mixed element types when using that option.

#### Example

To read from a Cloudant database as source and write documents to a Kafka topic, here is a minimal `connect-cloudant-source.properties`, using the default IAM authentication:

```
name=cloudant-source
connector.class=com.ibm.cloudant.kafka.connect.CloudantSourceConnector
topics=mytopic
cloudant.url=https://some-uuid.cloudantnosqldb.appdomain.cloud
cloudant.db=my-db
cloudant.apikey=my-apikey
```

### Cloudant as sink

In addition to those properties related to authentication, the Cloudant sink connector supports the following properties:

Parameter | Value | Required | Default value | Description
---:|:---|:---|:---|:---
name|cloudant-sink|YES|None|A unique name to identify the connector with.
connector.class|com.ibm.cloudant.kafka.connect.CloudantSinkConnector|YES|None|The connector class name.
topics|\<topic1\>,\<topic2\>,..|YES|None|The list of topics you want to consume messages from.
cloudant.url|https://\<your-account\>.cloudant.com|YES|None|The Cloudant server to write documents to.
cloudant.db|\<your-db\>|YES|None|The Cloudant database to write documents to.
tasks.max|5|NO|1|The number of concurrent threads to use for parallel bulk insert into Cloudant.
batch.size|400|NO|1000|The maximum number of documents to commit with a single bulk insert.
replication|false|NO|false|Managed object schema in sink database <br>*true: duplicate objects from source <br>false: adjust objects from source (\_id = [\<topic-name\>\_\<partition\>\_\<offset>\_\<sourceCloudantObjectId\>], kc\_schema = Kafka value schema)*

#### Example

To consume messages from a Kafka topic and save as documents into a Cloudant database, here is a minimal `connect-cloudant-sink.properties`, using the default IAM authentication:

```
name=cloudant-sink
connector.class=com.ibm.cloudant.kafka.connect.CloudantSinkConnector
topics=mytopic
cloudant.url=https://some-uuid.cloudantnosqldb.appdomain.cloud
cloudant.db=my-db
cloudant.apikey=my-apikey
```

## Usage

The kafka-cloudant-connect jar is available to download from [Maven Central](http://search.maven.org/#search|ga|1|g:"com.cloudant"%20AND%20a:"kafka-connect-cloudant").

Kafka will use the $CLASSPATH to locate available connectors. Make sure to add the connector library to your $CLASSPATH first.

Connector execution in Kafka is available through scripts in the Kafka install path:

`$kafka_home/bin/connect-standalone.sh` or `$kafka_home/bin/connect-distributed.sh`

Use the appropriate configuration files for standalone or distributed execution with Cloudant as source, as sink, or both.

For example:
- standalone execution with Cloudant as source:

  ```
  $kafka_home/bin/connect-standalone.sh connect-standalone.properties connect-cloudant-source.properties
  ```

- standalone execution with Cloudant as sink:

  ```
  $kafka_home/bin/connect-standalone.sh connect-standalone.properties connect-cloudant-sink.properties
  ```

- standalone execution with multiple configurations, one using Cloudant as source and one using Cloudant as sink:

  ```
  $kafka_home/bin/connect-standalone.sh connect-standalone.properties connect-cloudant-source.properties connect-cloudant-sink.properties
  ```

Any number of connector configurations can be passed to the executing script.

INFO level logging is configured by default to the console. To change log levels or settings, work with

`$kafka_home/config/connect-log4j.properties`

and add log settings like

`log4j.logger.com.ibm.cloudant.kafka=DEBUG, stdout`
