pipeline {
    agent any
    stages {
//        stage('Build') {
//            steps {
//                sh 'rm -rf build && mkdir build'
//                dir('build') {
//                    sh 'cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SANITIZER=Address ..'
//                    sh 'make -j4'
//                }
//            }
//        }
//
//        stage('Test') {
//            steps {
//                dir('build') {
//                    sh 'LD_PRELOAD="" make -j4 check'
//                    step([$class: 'XUnitBuilder', testTimeMargin: '3000', thresholdMode: 1, thresholds: [[$class: 'FailedThreshold', failureNewThreshold: '', failureThreshold: '', unstableNewThreshold: '', unstableThreshold: ''], [$class: 'SkippedThreshold', failureNewThreshold: '', failureThreshold: '', unstableNewThreshold: '', unstableThreshold: '']], tools: [[$class: 'GoogleTestType', deleteOutputFiles: true, failIfNotNew: true, pattern: 'test/*_test.xml', skipNoTestFiles: false, stopProcessingIfError: true]]])
//                    sh 'make install'
//                    sh 'LD_PRELOAD="" bash ../script/testing/psql/psql_test.sh || echo "failed, continuing"' // sometimes core dumps
//                    sh 'LD_PRELOAD="" python ../script/validators/jdbc_validator.py'
//                }
//            }
//        }

        stage('Other platforms') {
            parallel {
                stage('ubuntu-trusty') {
                    agent { dockerfile { filename 'script/docker/ubuntu-trusty-amd64/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo trusty'
                    }
                }
                
                stage('debian-stretch') {
                    agent { dockerfile { filename 'script/docker/debian-stretch-amd64/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo stretch'
                    }
                }

                stage('fedora24') {
                    agent { dockerfile { filename 'script/docker/fedora24-amd64/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo fedora24'
                    }
                }

                stage('fedora25') {
                    agent { dockerfile { filename 'script/docker/fedora25-amd64/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo fedora25'
                    }
                }

                stage('fedora26') {
                    agent { dockerfile { filename 'script/docker/fedora26-amd64/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo fedora26'
                    }
                }

                stage('centos7') {
                    agent { dockerfile { filename 'script/docker/centos7-amd64/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo centos7'
                    }
                }
            }
        }
    }
}
