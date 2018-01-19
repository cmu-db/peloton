pipeline {
    agent any
    stages {
        stage('Build') {
            parallel {
                stage('Ubuntu Xenial/gcc-5.4.0/llvm-3.8.0/Debug/Test') {
                    agent {
                        docker {
                            image 'ubuntu:xenial'
                            args "-v /var/lib/jenkins/jobs/crd/jobs/${env.JOB_BASE_NAME}/builds/${env.BUILD_ID}:/job"
                        }
                    }
                    steps {
                        sh 'sudo /bin/bash -c "source ./peloton/script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build && cd build'
                        sh 'cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False ..'
                        sh 'make -j4'
                        sh 'make check -j4'
                        sh 'make benchmark -j4'
                        sh 'make install'
                        sh 'bash ../script/testing/psql/psql_test.sh'
                        sh 'python ../script/validators/jdbc_validator.py'
                    }
                }

                // stage('Ubuntu Trusty/gcc-4.8.4/llvm-3.7.1') {
                //     agent { docker { image 'ubuntu:trusty' } }
                //     steps {
                //         sh 'lsb_release -a'
                //     }
                // }

                // stage('Fedora 26/gcc-7.1.1/llvm-4.0.1') {
                //     agent { docker { image 'fedora:26' } }
                //     steps {
                //         sh 'lsb_release -a'
                //     }
                // }

                // stage('Fedora 27/gcc-7.2.1/llvm-4.0.1') {
                //     agent { docker { image 'fedora:27' } }
                //     steps {
                //         sh 'lsb_release -a'
                //     }
                // }

                // stage('CentOS 7/gcc-4.8.5/llvm-3.9.1') {
                //     agent { docker { image 'centos:7' } }
                //     steps {
                //         sh 'lsb_release -a'
                //     }
                // }

                // stage('Ubuntu Xenial/clang-3.8.0/llvm-3.8.0') {
                //     agent { docker { image 'ubuntu:xenial' } }
                //     steps {
                //         sh 'apt-get -qq update && apt-get -qq -y --no-install-recommends install python-dev lsb-release sudo'
                //         sh '/bin/bash -c "source ./peloton/script/installation/packages.sh"'
                //         sh 'rm -rf /peloton/build && mkdir /peloton/build && cd /peloton/build && CC=clang-3.7 CXX=clang++-3.7 cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4 && make install'
                //         sh 'rm -rf /peloton/build'
                //         sh 'rm -rf /peloton/build && mkdir /peloton/build && cd /peloton/build && CC=clang-3.7 CXX=clang++-3.7 cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4 && make install'
                //     }
                // }

                // stage('Ubuntu Trusty/clang-3.7.1/llvm-3.7.1') {
                //     agent { docker { image 'ubuntu:trusty' } }
                //     steps {
                //         sh 'lsb_release -a'
                //     }
                // }

                // stage('Fedora 26/clang-4.0.1/llvm-4.0.1') {
                //     agent { docker { image 'fedora:26' } }
                //     steps {
                //         sh 'lsb_release -a'
                //     }
                // }

                // stage('Fedora 27/clang-4.0.1/llvm-4.0.1') {
                //     agent { docker { image 'fedora:27' } }
                //     steps {
                //         sh 'lsb_release -a'
                //     }
                // }

                // stage('CentOS 7/clang-??/llvm-3.9.1') {
                //     agent { docker { image 'centos:7' } }
                //     steps {
                //         sh 'lsb_release -a'
                //     }
                // }
            }
        }
    }
}
