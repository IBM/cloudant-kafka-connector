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

def prefixlessTag(tag) {
    return tag.replaceFirst('v','')
}
def uploadUrl
def jarName

pipeline {
    agent {
        label 'sdks-executor'
    }

    stages {
        stage('Build') {
            steps {
                sh './gradlew clean assemble'
            }
        }

        stage('QA') {
            steps {
                withCredentials([string(credentialsId: 'testServerIamApiKey',
                        variable: 'APIKEY')]) {
                    sh './gradlew -Dcloudant.url=$SDKS_TEST_SERVER_URL -Dcloudant.auth.url=$SDKS_TEST_IAM_URL -Dcloudant.apikey=$APIKEY test'
                }
            }
            post {
                always {
                    junit (
                        testResults: '**/build/test-results/test/*.xml'
                    )
                }
            }
        }

        // Publish tags
        stage('Publish release') {    
            // Publish releases for semver tags that equal the verison in the file
            when {
                allOf {
                    buildingTag()
                    tag pattern: /${env.SVRE_PRE_RELEASE_TAG}/, comparator: 'REGEXP'
                    tag pattern: 'v' + readFile('VERSION').trim(), comparator : 'EQUALS'
                }
            }
            steps {
                // Create a GitHub release for the tag
                httpRequest authentication: 'gh-sdks-automation',
                            contentType: 'APPLICATION_JSON_UTF8',
                            customHeaders: [[name: 'Accept', value: 'application/vnd.github+json']],
                            httpMode: 'POST',
                            outputFile: 'release_response.json',
                            requestBody: """
                                {
                                    "tag_name": "${TAG_NAME}",
                                    "name": "${prefixlessTag(TAG_NAME)} (${new Date(TAG_TIMESTAMP as long).format('yyyy-MM-dd')})"
                                    "draft": false,
                                    "prerelease": true,
                                    "generate_release_notes": true
                                }
                                """.stripIndent(),
                            timeout: 60,
                            url: 'https://api.github.com/repos/IBM/cloudant-kafka-connector/releases',
                            validResponseCodes: '201',
                            wrapAsMultipart: false
                script {
                    jarName = "cloudant-kafka-connector-${prefixlessTag(TAG_NAME)}.jar"
                    // Process the release response to get the asset upload_url
                    def responseJson = readJSON file: 'release_response.json'
                    // Replace the path parameter template with a name
                    uploadUrl = responseJson.upload_url.replace('{?name,label}',"?name=${jarName}")
                }
                // Upload the asset to the release
                httpRequest authentication: 'gh-sdks-automation',
                            customHeaders: [[name: 'Accept', value: 'application/vnd.github+json'],[name: 'Content-Type', value: 'application/java-archive']],
                            httpMode: 'POST',
                            timeout: 60,
                            uploadFile: "build/libs/${jarName}",
                            url: uploadUrl,
                            validResponseCodes: '201',
                            wrapAsMultipart: false
            }
        }
    }
}
