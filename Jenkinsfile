#!groovy

/*
 * Copyright © 2018 IBM Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

stage('Build') {
    // Checkout, build and assemble the source and doc
    node('sdks-kafka-executor') {
        checkout scm
        sh './gradlew clean assemble'
        stash name: 'built'
    }
}

stage('QA') {
    node('sdks-kafka-executor') {
        unstash name: 'built'
        withCredentials([usernamePassword(credentialsId: 'clientlibs-test',
                usernameVariable: 'DB_USER',
                passwordVariable: 'DB_PASSWORD')]) {

            try {
                sh './gradlew -Dcloudant.account=https://clientlibs-test.cloudant.com -Dcloudant.user=$DB_USER -Dcloudant.pass=$DB_PASSWORD test'
            } finally {
                junit '**/build/test-results/test/*.xml'
            }
        }
    }
}

// Publish the master branch
stage('Publish') {
    if (env.BRANCH_NAME == "master") {
        node('sdks-kafka-executor') {
            unstash name: 'built'
            // read the version name and determine if it is a release build
            version = readFile('VERSION').trim()
            isReleaseVersion = !version.toUpperCase(Locale.ENGLISH).contains("SNAPSHOT")

            // Upload using the ossrh creds (upload destination logic is in build.gradle)
            withCredentials([usernamePassword(credentialsId: 'ossrh-creds', passwordVariable: 'OSSRH_PASSWORD', usernameVariable: 'OSSRH_USER'), usernamePassword(credentialsId: 'signing-creds', passwordVariable: 'KEY_PASSWORD', usernameVariable: 'KEY_ID'), file(credentialsId: 'signing-key', variable: 'SIGNING_FILE')]) {
                sh './gradlew -Dsigning.keyId=$KEY_ID -Dsigning.password=$KEY_PASSWORD -Dsigning.secretKeyRingFile=$SIGNING_FILE -DossrhUsername=$OSSRH_USER -DossrhPassword=$OSSRH_PASSWORD upload'
            }
        }
    }
    gitTagAndPublish {
        isDraft=true
        releaseApiUrl='https://api.github.com/repos/cloudant-labs/kafka-connect-cloudant/releases'
    }
}
