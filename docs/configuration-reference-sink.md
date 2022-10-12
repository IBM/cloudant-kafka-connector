# Database

## `cloudant.url`
Cloudant database connection URL (eg `https://<uuid>.cloudantnosqldb.appdomain.cloud`)

* Type: `string`
* Valid Values: `<any URL>`

## `cloudant.db`
Cloudant database name (for sink connector it will be created if it does not exist)

* Type: `string`

# Kafka

## `topics`
Kafka topic list

* Type: `list`

## `batch.size`
Size of batches to send to Cloudant _bulk_docs endpoint

* Type: `int`
* Default: `1000`
* Valid Values: `[1,...,2000]`

# Authentication

## `cloudant.auth.type`
Cloudant authentication method (or type)

* Type: `string`
* Default: `iam`
* Valid Values: `[iam, couchdb_session, basic, noauth, bearertoken, container, vpc]`

## `cloudant.apikey`
Cloudant database IAM API key, for use with "iam" authentication

* Type: `password`

## `cloudant.username`
Cloudant username, for use with "couchdb_session" or "basic" authentication

* Type: `string`

## `cloudant.password`
Cloudant password, for use with "couchdb_session" or "basic" authentication

* Type: `password`

## `cloudant.bearer.token`
Cloudant bearer token, for use with "bearerToken" authentication

* Type: `string`

## `cloudant.iam.profile.id`
Cloudant IAM profile ID, for use with "container" or "vpc" authentication

* Type: `string`

## `cloudant.iam.profile.name`
Cloudant IAM profile name, for use with "container" authentication

* Type: `string`

## `cloudant.cr.token.filename`
Cloudant CR token filename, for use with "container" authentication

* Type: `string`

## `cloudant.iam.profile.crn`
Clouant IAM profile CRN, for use with "vpc" authentication

* Type: `string`

## `cloudant.auth.url`
Cloudant auth URL, for use with "iam", "container", or "vpc" authentication

* Type: `string`
* Valid Values: `<any URL>`

## `cloudant.scope`
Cloudant scope, for use with "iam", "container", or "vpc" authentication

* Type: `string`

## `cloudant.client.id`
Cloudant client ID, for use with "iam", "container", or "vpc" authentication

* Type: `string`

## `cloudant.client.secret`
Cloudant client secret, for use with "iam", "container", or "vpc" authentication

* Type: `string`

