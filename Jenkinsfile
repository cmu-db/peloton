pipeline {
    agent any
    stages {
        stage('Build') {
            parallel {
                // begin gcc builds
                // NOTE: this next stage is special because it copies the test results out of the container
                stage('Ubuntu Xenial/gcc-5.4.0/llvm-3.7.1 (Debug)') {
                    agent {
                        docker {
                            image 'ubuntu:xenial'
                            // args '-v ${WORKSPACE}/../builds/${BUILD_ID}:/job:rw'
                        }
                    }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SANITIZER=Address -DCOVERALLS=False .. && make -j4'
                        sh 'cd build && make check -j4'
                        // sh 'cd build && cp -pr test /job/' // special tests collection step just for this stage
                        sh 'cd build && make install'
                        sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                        sh 'sudo apt-get -qq update && sudo apt-get -qq -y --no-install-recommends install wget default-jdk default-jre' // prerequisites for jdbc_validator
                        sh 'cd build && python ../script/validators/jdbc_validator.py'
                        sh 'cd build && python ../script/testing/junit/run_junit.py'
                    }
                }

                stage('Ubuntu Xenial/gcc-5.4.0/llvm-3.7.1 (Release)') {
                    agent { docker { image 'ubuntu:xenial' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                        sh 'cd build && make check -j4'
                        sh 'cd build && make install'
                        sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                        sh 'sudo apt-get -qq update && sudo apt-get -qq -y --no-install-recommends install wget default-jdk default-jre' // prerequisites for jdbc_validator
                        sh 'cd build && python ../script/validators/jdbc_validator.py'
                        sh 'cd build && python ../script/testing/junit/run_junit.py'
                    }
                }

                stage('Ubuntu Trusty/gcc-4.8.4/llvm-3.7.1 (Debug)') {
                    agent { docker { image 'ubuntu:trusty' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SANITIZER=Address -DCOVERALLS=False .. && make -j4'
                        sh 'cd build && make check -j4'
                        sh 'cd build && make install'
                        sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                        sh 'sudo apt-get -qq update && sudo apt-get -qq -y --no-install-recommends install wget default-jdk default-jre' // prerequisites for jdbc_validator
                        sh 'cd build && python ../script/validators/jdbc_validator.py'
                        sh 'cd build && python ../script/testing/junit/run_junit.py'
                    }
                }

                stage('Ubuntu Trusty/gcc-4.8.4/llvm-3.7.1 (Release)') {
                    agent { docker { image 'ubuntu:trusty' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                        sh 'cd build && make check -j4'
                        sh 'cd build && make install'
                        sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                        sh 'sudo apt-get -qq update && sudo apt-get -qq -y --no-install-recommends install wget default-jdk default-jre' // prerequisites for jdbc_validator
                        sh 'cd build && python ../script/validators/jdbc_validator.py'
                        sh 'cd build && python ../script/testing/junit/run_junit.py'
                    }
                }

                stage('Ubuntu Xenial/gcc-5.4.0/llvm-3.7.1 (LOG_LEVEL_TRACE)') {
                    agent { docker { image 'ubuntu:xenial' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SANITIZER=Address -DCMAKE_CXX_FLAGS="-DLOG_LEVEL=LOG_LEVEL_TRACE" -DCOVERALLS=False .. && make -j4'
                    }
                }

                // stage('Debian Stretch/gcc-6.3.0/llvm-3.8.1 (Debug)') {
                //     agent { docker { image 'debian:stretch' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SANITIZER=Address -DCOVERALLS=False .. && make -j4'
                //         sh 'cd build && make check -j4'
                //         sh 'cd build && make install'
                //         sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                //         sh 'sudo apt-get -qq update && sudo apt-get -qq -y --no-install-recommends install wget default-jdk default-jre' // prerequisites for jdbc_validator
                //         sh 'cd build && python ../script/validators/jdbc_validator.py'
		//         sh 'cd build && python ../script/testing/junit/run_junit.py'
                //     }
                // }

                // stage('Debian Stretch/gcc-6.3.0/llvm-3.8.1 (Release)') {
                //     agent { docker { image 'debian:stretch' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                //         sh 'cd build && make check -j4'
                //         sh 'cd build && make install'
                //         sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                //         sh 'sudo apt-get -qq update && sudo apt-get -qq -y --no-install-recommends install wget default-jdk default-jre' // prerequisites for jdbc_validator
                //         sh 'cd build && python ../script/validators/jdbc_validator.py'
		//         sh 'cd build && python ../script/testing/junit/run_junit.py'
                //     }
                // }

                // stage('Fedora 26/gcc-7.1.1/llvm-4.0.1 (Debug)') {
                //     agent { docker { image 'fedora:26' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SANITIZER=Address -DCOVERALLS=False .. && make -j4'
                //         sh 'cd build && make check -j4'
                //         sh 'cd build && make install'
                //         sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                //         sh 'sudo dnf -q install -y wget java-devel' // prerequisites for jdbc_validator
                //         sh 'cd build && python ../script/validators/jdbc_validator.py'
		//         sh 'cd build && python ../script/testing/junit/run_junit.py'
                //     }
                // }

                // stage('Fedora 26/gcc-7.1.1/llvm-4.0.1 (Release)') {
                //     agent { docker { image 'fedora:26' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                //         sh 'cd build && make check -j4'
                //         sh 'cd build && make install'
                //         sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                //         sh 'sudo dnf -q install -y wget java-devel' // prerequisites for jdbc_validator
                //         sh 'cd build && python ../script/validators/jdbc_validator.py'
		//         sh 'cd build && python ../script/testing/junit/run_junit.py'
                //     }
                // }

                // stage('Fedora 27/gcc-7.2.1/llvm-4.0.1 (Debug)') {
                //     agent { docker { image 'fedora:27' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python ./script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && PATH=/usr/lib64/llvm4.0/bin:$PATH cmake -DCMAKE_CXX_FLAGS="-isystem /usr/include/llvm4.0" -DCMAKE_BUILD_TYPE=Debug -DUSE_SANITIZER=Address -DCOVERALLS=False .. && make -j4'
                //         sh 'cd build && make check -j4'
                //         sh 'cd build && make install'
                //         sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                //         sh 'sudo dnf -q install -y wget java-devel' // prerequisites for jdbc_validator
                //         sh 'cd build && python ../script/validators/jdbc_validator.py'
		//         sh 'cd build && python ../script/testing/junit/run_junit.py'
                //     }
                // }

                // stage('Fedora 27/gcc-7.2.1/llvm-4.0.1 (Release)') {
                //     agent { docker { image 'fedora:27' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python ./script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && PATH=/usr/lib64/llvm4.0/bin:$PATH cmake -DCMAKE_CXX_FLAGS="-isystem /usr/include/llvm4.0" -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                //         sh 'cd build && make check -j4'
                //         sh 'cd build && make install'
                //         sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                //         sh 'sudo dnf -q install -y wget java-devel' // prerequisites for jdbc_validator
                //         sh 'cd build && python ../script/validators/jdbc_validator.py'
		//         sh 'cd build && python ../script/testing/junit/run_junit.py'
                //     }
                // }

                // stage('CentOS 7/gcc-4.8.5/llvm-3.9.1 (Debug)') {
                //     agent { docker { image 'centos:7' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && cmake3 -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4'
                //         sh 'cd build && make check -j4'
                //         sh 'cd build && make install'
                //         sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                //         sh 'sudo yum -q install -y wget java-devel' // prerequisites for jdbc_validator
                //         sh 'cd build && python ../script/validators/jdbc_validator.py'
		//         sh 'cd build && python ../script/testing/junit/run_junit.py'
                //     }
                // }

                // stage('CentOS 7/gcc-4.8.5/llvm-3.9.1 (Release)') {
                //     agent { docker { image 'centos:7' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && cmake3 -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                //         sh 'cd build && make check -j4'
                //         sh 'cd build && make install'
                //         sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                //         sh 'sudo yum -q install -y wget java-devel' // prerequisites for jdbc_validator
                //         sh 'cd build && python ../script/validators/jdbc_validator.py'
		//         sh 'cd build && python ../script/testing/junit/run_junit.py'
                //     }
                // }
                // end gcc builds

                // begin clang builds
                // stage('Ubuntu Xenial/clang-3.7.1/llvm-3.7.1 (Debug)') {
                //     agent { docker { image 'ubuntu:xenial' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./peloton/script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && CC=clang-3.7 CXX=clang++-3.7 cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4 && make install'
                //     }
                // }

                // stage('Ubuntu Xenial/clang-3.7.1/llvm-3.7.1 (Release)') {
                //     agent { docker { image 'ubuntu:xenial' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./peloton/script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && CC=clang-3.7 CXX=clang++-3.7 cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4 && make install'
                //     }
                // }

                // stage('Ubuntu Trusty/clang-3.7.1/llvm-3.7.1 (Debug)') {
                //     agent { docker { image 'ubuntu:trusty' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && CC=clang-3.7 CXX=clang++-3.7 cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4'
                //     }
                // }

                // stage('Ubuntu Trusty/clang-3.7.1/llvm-3.7.1 (Release)') {
                //     agent { docker { image 'ubuntu:trusty' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && CC=clang-3.7 CXX=clang++-3.7 cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                //     }
                // }

                // stage('Fedora 26/clang-4.0.1/llvm-4.0.1 (Debug)') {
                //     agent { docker { image 'fedora:26' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && CC=clang CXX=clang++ cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4'
                //     }
                // }

                // stage('Fedora 26/clang-4.0.1/llvm-4.0.1 (Release)') {
                //     agent { docker { image 'fedora:26' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && CC=clang CXX=clang++ cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                //     }
                // }

                // stage('Fedora 27/clang-4.0.1/llvm-4.0.1 (Debug)') {
                //     agent { docker { image 'fedora:27' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && CC=clang CXX=clang++ cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4'
                //     }
                // }

                // stage('Fedora 27/clang-4.0.1/llvm-4.0.1 (Release)') {
                //     agent { docker { image 'fedora:27' } }
                //     steps {
                //         sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                //         sh 'python script/validators/source_validator.py'
                //         sh 'mkdir build'
                //         sh 'cd build && CC=clang CXX=clang++ cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                //     }
                // }

                // Omit this configuration for now since the corresponding version of clang does not seem to be available on this platform
                // stage('CentOS 7/clang-??/llvm-3.9.1') {
                //     agent { docker { image 'centos:7' } }
                //     steps {
                //         sh 'lsb_release -a'
                //     }
                // }
                // end clang builds
            }
        }
    }

    // Process test results from the first build stage
    // post {
    //     always {
    //         dir("${WORKSPACE}/../builds/${BUILD_ID}") {
    //             step([$class: 'XUnitBuilder', testTimeMargin: '3000', thresholdMode: 1, thresholds: [[$class: 'FailedThreshold', failureNewThreshold: '', failureThreshold: '', unstableNewThreshold: '', unstableThreshold: ''], [$class: 'SkippedThreshold', failureNewThreshold: '', failureThreshold: '', unstableNewThreshold: '', unstableThreshold: '']], tools: [[$class: 'GoogleTestType', deleteOutputFiles: true, failIfNotNew: true, pattern: 'test/*_test.xml', skipNoTestFiles: false, stopProcessingIfError: true]]])
    //         }
    //     }
    // }
}
