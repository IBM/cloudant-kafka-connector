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

### Cloudant as source

To read from a Cloudant database as source and write documents to a Kafka topic, create a source configuration file 

`connect-cloudant-source.properties`

with the following parameters:

| Parameter | Type | Required? | Example | Default value |
| --------- | ---- | --------- | ------- | ------------- |
| cloudant.db.url | String | YES | "https://kache.cloudant.com/database" | None
| cloudant.db.username | String | YES | "kache" | None
| cloudant.db.password | String | YES | "myPassword" | None
| cloudant.db.since | String | NO | "1-g1AAAAETeJzLYWBgYMlgTmGQT0lKzi9KdUhJMtZLykxPyilN1UvOyS9NScwr0ctLLckBKmRKZEiy____f1YiA6oWM9xakhyAZFI9Nl2muHXlsQBJhgYgBdS4H0OnCSGdByA6_2dlMCcy5gIF2M1SzZOM0lKQTTHCakoWAGOUWq8" | "0"
| kafka.topic | String | YES | "cloudant-topic" | None

## Usage

## Building from Source

The project requires Java 8 and Maven 2 to build from source. Execute the following command in the project directory:

```
mvn clean install
```

## Test execution

Junit tests are available in `src/test/java`. To execute, please modify values in `src/test/resources`, including:

- log4j.properties (optional)
- test.properties (required)

The settings in test.properties have to include Cloudant database credentials and Kafka topic details. See file for details.

