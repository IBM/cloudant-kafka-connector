# Database

``cloudant.url``

Cloudant database connection URL (eg `https://<uuid>.cloudantnosqldb.appdomain.cloud`)

* Type: string
* Valid Values: <any URL>
* Importance: high

``cloudant.db``

Cloudant database name (for sink connector it will be created if it does not exist)

* Type: string
* Importance: high

``cloudant.since``

Last update sequence identifier to resume from. Identified sequence number is included in the result set. See "last_seq" parameter in Cloudant _changes API documentation for details.

* Type: string
* Default: 0
* Importance: low

# Kafka

``topics``

Kafka topic list

* Type: list
* Importance: high

``batch.size``

Size of batches to retrieve from Cloudant _changes endpoint

* Type: int
* Default: 1000
* Valid Values: [1,...,20000]
* Importance: medium

# Authentication

``cloudant.auth.type``

Cloudant authentication method (or type)

* Type: string
* Default: iam
* Valid Values: [iam, couchdb_session, basic, noauth, bearertoken, container, vpc]
* Importance: high

``cloudant.apikey``

Cloudant database IAM API key, for use with "iam" authentication

* Type: password
* Default: null
* Importance: high

``cloudant.username``

Cloudant username, for use with "couchdb_session" or "basic" authentication

* Type: string
* Default: null
* Importance: low

``cloudant.password``

Cloudant password, for use with "couchdb_session" or "basic" authentication

* Type: password
* Default: null
* Importance: low

``cloudant.bearer.token``

Cloudant bearer token, for use with "bearerToken" authentication

* Type: string
* Default: null
* Importance: low

``cloudant.iam.profile.id``

Cloudant IAM profile ID, for use with "container" or "vpc" authentication

* Type: string
* Default: null
* Importance: low

``cloudant.iam.profile.name``

Cloudant IAM profile name, for use with "container" authentication

* Type: string
* Default: null
* Importance: low

``cloudant.cr.token.filename``

Cloudant CR token filename, for use with "container" authentication

* Type: string
* Default: null
* Importance: low

``cloudant.iam.profile.crn``

Clouant IAM profile CRN, for use with "vpc" authentication

* Type: string
* Default: null
* Importance: low

``cloudant.auth.url``

Cloudant auth URL, for use with "iam", "container", or "vpc" authentication

* Type: string
* Default: null
* Valid Values: <any URL>
* Importance: low

``cloudant.scope``

Cloudant scope, for use with "iam", "container", or "vpc" authentication

* Type: string
* Default: null
* Importance: low

``cloudant.client.id``

Cloudant client ID, for use with "iam", "container", or "vpc" authentication

* Type: string
* Default: null
* Importance: low

``cloudant.client.secret``

Cloudant client secret, for use with "iam", "container", or "vpc" authentication

* Type: string
* Default: null
* Importance: low
