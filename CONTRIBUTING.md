# Contributing

## Issues

Please [read these guidelines](http://ibm.biz/cdt-issue-guide) before opening an issue.
If you still need to open an issue then we ask that you complete the template as
fully as possible.

## Pull requests

We welcome pull requests, but ask contributors to keep in mind the following:

* Only PRs with the template completed will be accepted
* We will not accept PRs for user specific functionality

### Developer Certificate of Origin

In order for us to accept pull-requests, the contributor must sign-off a
[Developer Certificate of Origin (DCO)](DCO1.1.txt). This clarifies the
intellectual property license granted with any contribution. It is for your
protection as a Contributor as well as the protection of IBM and its customers;
it does not change your rights to use your own Contributions for any other purpose.

Please read the agreement and acknowledge it by ticking the appropriate box in the PR
 text, for example:

- [x] Tick to sign-off your agreement to the Developer Certificate of Origin (DCO) 1.1

<!-- Append library specific information here

## General information

## Requirements

- Java 8

## Building

Execute the following command in the project directory:

```sh
./gradlew clean assemble
```

## Testing

Junit tests are available in `src/test/java`.

To execute locally, please modify values in `src/test/resources`, including:

- log4j.properties (optional)
- test.properties (required)

The settings in `test.properties` have to include Cloudant database credentials and Kafka topic details as above.
At a minimum you will need to update the values of `cloudant.db.url` `cloudant.db.username` and `cloudant.db.password`.
The Cloudant credentials must have `_admin` permission as the database referenced by `cloudant.db.url` will be
created if it does not exist and will be deleted at the end of the tests.

```sh
./gradlew test
```
