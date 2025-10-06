#!groovy

/*
 * Copyright © 2018, 2023 IBM Corp. All rights reserved.
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
def zipUploadUrl
def zipName

pipeline {
    agent {
        kubernetes {
            // Temporarily pin the agent until sb361 signing issue is resolved
            yaml kubePodTemplate(name: 'full_jnlp.yaml', full_jnlp: 'sdks-full-agent:java-21')
        }
    }

    environment {
        ARTIFACTORY_CREDS = credentials('artifactory')
        ARTIFACTORY_URL = "${Artifactory.server('taas-artifactory').getUrl()}"
    }

    stages {
        stage('Detect Secrets') {
            steps {
                detectSecrets()
            }
        }

        stage('Build') {
            steps {
                sh 'gradle clean assemble'
                withCredentials([certificate(credentialsId: 'cldtsdks-signing-cert', keystoreVariable: 'CODE_SIGNING_PFX_FILE', passwordVariable: 'CODE_SIGNING_P12_PASSWORD')
                ]) {
                    sh 'setup-garasign-client'
                    sh 'gradle signDistZip'
                }
            }
        }

        stage('QA') {
            steps {
                withCredentials([string(credentialsId: 'testServerIamApiKey',
                        variable: 'APIKEY')]) {
                    sh 'gradle -Dcloudant.url=$SDKS_TEST_SERVER_URL -Dcloudant.auth.url=$SDKS_TEST_IAM_URL -Dcloudant.apikey=$APIKEY test'
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

        stage('SonarQube analysis') {
            when {
                anyOf {
                    changeRequest()
                    expression { env.BRANCH_IS_PRIMARY }
                }
                not {
                    changeRequest branch: 'dependabot*', comparator: 'GLOB'
                }
            }
            environment {
                scannerHome = tool 'SonarQubeScanner'
            }
            steps {
                withSonarQubeEnv(installationName: 'SonarQubeServer') {
                    sh 'gradle sonar -Dsonar.qualitygate.wait=true -Dsonar.projectKey=cloudant-kafka-connector'
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
                                    "name": "${prefixlessTag(TAG_NAME)} (${new Date(TAG_TIMESTAMP as long).format('yyyy-MM-dd')})",
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
                    zipName = "cloudant-kafka-connector-${prefixlessTag(TAG_NAME)}.zip"
                    // Process the release response to get the asset upload_url
                    def responseJson = readJSON file: 'release_response.json'
                    // Replace the path parameter template with a name
                    zipUploadUrl = responseJson.upload_url.replace('{?name,label}',"?name=${zipName}")
                }
                // Upload the asset to the release
                httpRequest authentication: 'gh-sdks-automation',
                            customHeaders: [[name: 'Accept', value: 'application/vnd.github+json'],[name: 'Content-Type', value: 'application/zip']],
                            httpMode: 'POST',
                            timeout: 60,
                            uploadFile: "build/distributions/${zipName}",
                            url: zipUploadUrl,
                            validResponseCodes: '201',
                            wrapAsMultipart: false
            }
        }
    }
}
