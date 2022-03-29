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

To execute locally, supply credentials to the gradle `test` task, eg

```sh
./gradlew -Dcloudant.auth.type=basic -Dcloudant.url=https://<your-account>.cloudant.com -Dcloudant.username=<your-username> -Dcloudant.password=<your-password> test

```

for basic authentication, or

```sh
./gradlew -Dcloudant.auth.type=iam -Dcloudant.url=https://<your-account>.cloudant.com -Dcloudant.apikey=<your-apikey> test

```

fo IAM authentication.
