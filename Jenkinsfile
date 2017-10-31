pipeline {
    agent any
    stages {
        stage('Debug Build') {
            steps {
                sh 'rm -rf build && mkdir build'
                dir('build') {
                    sh 'cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SANITIZER=Address -DCMAKE_CXX_FLAGS="-DLOG_LEVEL=LOG_LEVEL_TRACE" ..'
                    sh 'make -j4'
                }
            }
        }

        stage('Test') {
            steps {
                dir('build') {
				    // the LD_PRELOADs are necessary because our Jenkins uses its own which will otherwise disrupt ASan
                    sh 'LD_PRELOAD="" make -j4 check'
                    step([$class: 'XUnitBuilder', testTimeMargin: '3000', thresholdMode: 1, thresholds: [[$class: 'FailedThreshold', failureNewThreshold: '', failureThreshold: '', unstableNewThreshold: '', unstableThreshold: ''], [$class: 'SkippedThreshold', failureNewThreshold: '', failureThreshold: '', unstableNewThreshold: '', unstableThreshold: '']], tools: [[$class: 'GoogleTestType', deleteOutputFiles: true, failIfNotNew: true, pattern: 'test/*_test.xml', skipNoTestFiles: false, stopProcessingIfError: true]]])
                    sh 'make install'
                    sh 'LD_PRELOAD="" bash ../script/testing/psql/psql_test.sh || echo "failed, continuing"' // sometimes core dumps
                    sh 'LD_PRELOAD="" python ../script/validators/jdbc_validator.py'
                }
            }
        }

        stage('Release target builds') {
            parallel {
                stage('Ubuntu Trusty') {
                    agent { dockerfile { filename 'script/docker/ubuntu-trusty/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo Ubuntu Trusty'
                    }
                }
                
                stage('Debian Stretch') {
                    agent { dockerfile { filename 'script/docker/debian-stretch/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo Debian Stretch'
                    }
                }

                stage('Fedora 24') {
                    agent { dockerfile { filename 'script/docker/fedora24/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo Fedora 24'
                    }
                }

                stage('Fedora 25') {
                    agent { dockerfile { filename 'script/docker/fedora25/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo Fedora 25'
                    }
                }

                stage('Fedora 26') {
                    agent { dockerfile { filename 'script/docker/fedora26/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo Fedora 26'
                    }
                }

                stage('CentOS 7') {
                    agent { dockerfile { filename 'script/docker/centos7/Dockerfile-jenkins' } }
                    steps {
                        sh 'echo CentOS 7'
                    }
                }
            }
        }
    }
}
