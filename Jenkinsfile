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

pipeline {
    agent {
        kubernetes {
            yaml kubePodTemplate(name: 'full_jnlp.yaml')
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
                publishGitHubRelease(
                    tag: env.TAG_NAME,
                    title: "${prefixlessTag(TAG_NAME)} (${new Date(TAG_TIMESTAMP as long).format('yyyy-MM-dd')})",
                    preRelease: true,
                    assets: [
                        [
                            name: "cloudant-kafka-connector-${prefixlessTag(TAG_NAME)}.zip",
                            path: "build/distributions/cloudant-kafka-connector-${prefixlessTag(TAG_NAME)}.zip",
                            contentType: 'application/zip'
                        ]
                    ]
                )
            }
        }
    }
}
