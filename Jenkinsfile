pipeline {
    agent none
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

                stage('Ubuntu Trusty/gcc-5.4.0/llvm-3.7.1 (Debug)') {
                    agent { docker { image 'ubuntu:trusty' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'sudo apt-get -y install software-properties-common'
                        sh 'sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test'
                        sh 'sudo apt-get update'
                        sh 'sudo apt-get -y install gcc-5 g++-5'
                        sh 'sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-5 1 --slave /usr/bin/g++ g++ /usr/bin/g++-5'
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

                stage('Ubuntu Trusty/gcc-5.4.0/llvm-3.7.1 (Release)') {
                    agent { docker { image 'ubuntu:trusty' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'sudo apt-get -y install software-properties-common'
                        sh 'sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test'
                        sh 'sudo apt-get update'
                        sh 'sudo apt-get -y install gcc-5 g++-5'
                        sh 'sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-5 1 --slave /usr/bin/g++ g++ /usr/bin/g++-5'
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

                stage('macOS 10.13/Apple LLVM version 9.1.0 (Debug)') {
                    agent { label 'macos' }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python script/validators/source_validator.py'
                        sh 'export LLVM_DIR=/usr/local/Cellar/llvm@3.9/3.9.1_1'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_SANITIZER=Address -DCOVERALLS=False .. && make -j4'
                        sh 'cd build && ASAN_OPTIONS=detect_container_overflow=0 make check -j4'
                        sh 'cd build && make install'
                        sh 'cd build && bash ../script/testing/psql/psql_test.sh'
                        sh 'cd build && python ../script/validators/jdbc_validator.py'
                        sh 'cd build && ASAN_OPTIONS=detect_container_overflow=0 python ../script/testing/junit/run_junit.py'
                    }
                }
            }
        }
    }
}