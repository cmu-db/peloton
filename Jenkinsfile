pipeline {
    agent any
    stages {
        stage('Build') {
            parallel {
                // NOTE: this stage is special because it copies the test results out of the container
                stage('Ubuntu Xenial/gcc-5.4.0/llvm-3.8.0 (Debug/Test)') {
                    agent {
                        docker {
                            image 'ubuntu:xenial'
                            args "-v /var/lib/jenkins/jobs/crd/jobs/${env.JOB_BASE_NAME}/builds/${env.BUILD_ID}:/job"
                        }
                    }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && PATH=/usr/lib/llvm-3.8/bin:/usr/bin:$PATH cmake -DCMAKE_PREFIX_PATH=`llvm-config-3.8 --prefix` -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False -DUSE_SANITIZER=Address .. && make -j4'
                        sh 'cd build && make check -j4 && cp -pr test /job/'
                        sh 'cd build && make benchmark -j4'
                        sh 'cd build && make install'
                        sh 'bash ../script/testing/psql/psql_test.sh'
                        sh 'python ../script/validators/jdbc_validator.py'
                    }
                }

                stage('Ubuntu Xenial/gcc-5.4.0/llvm-3.8.0 (Release)') {
                    agent { docker { image 'ubuntu:xenial' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && PATH=/usr/lib/llvm-3.8/bin:/usr/bin:$PATH cmake -DCMAKE_PREFIX_PATH=`llvm-config-3.8 --prefix` -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                    }
                }

                stage('Ubuntu Trusty/gcc-4.8.4/llvm-3.7.1 (Debug/Test/LOG_LEVEL_TRACE)') {
                    agent { docker { image 'ubuntu:trusty' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False -DUSE_SANITIZER=Address -DCMAKE_CXX_FLAGS="-DLOG_LEVEL=LOG_LEVEL_TRACE" .. && make -j4'
                        // redirect output to /dev/null because it is voluminous
                        sh 'cd build && make check -j4 > /dev/null'
                        sh 'cd build && make benchmark -j4'
                        sh 'cd build && make install'
                        sh 'bash ../script/testing/psql/psql_test.sh'
                        sh 'python ../script/validators/jdbc_validator.py'
                    }
                }

                stage('Ubuntu Trusty/gcc-4.8.4/llvm-3.7.1 (Release/Test)') {
                    agent { docker { image 'ubuntu:trusty' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DUSE_SANITIZER=Address -DCOVERALLS=False .. && make -j4'
                        sh 'cd build && make check -j4'
                        sh 'cd build && make benchmark -j4'
                        sh 'cd build && make install'
                        sh 'bash ../script/testing/psql/psql_test.sh'
                        sh 'python ../script/validators/jdbc_validator.py'
                    }
                }

                stage('Fedora 26/gcc-7.1.1/llvm-4.0.1 (Debug)') {
                    agent { docker { image 'fedora:26' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4'
                    }
                }
                stage('Fedora 26/gcc-7.1.1/llvm-4.0.1 (Release)') {
                    agent { docker { image 'fedora:26' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                    }
                }

                stage('Fedora 27/gcc-7.2.1/llvm-4.0.1 (Debug)') {
                    agent { docker { image 'fedora:27' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4'
                    }
                }

                stage('Fedora 27/gcc-7.2.1/llvm-4.0.1 (Release)') {
                    agent { docker { image 'fedora:27' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                    }
                }

                stage('CentOS 7/gcc-4.8.5/llvm-3.9.1 (Debug)') {
                    agent { docker { image 'centos:7' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4'
                    }
                }
                stage('CentOS 7/gcc-4.8.5/llvm-3.9.1 (Release)') {
                    agent { docker { image 'centos:7' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                    }
                }

                stage('Ubuntu Xenial/clang-3.8.0/llvm-3.8.0 (Debug)') {
                    agent { docker { image 'ubuntu:xenial' } }
                    steps {
                        sh '/bin/bash -c "source ./peloton/script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && CC=clang-3.8 CXX=clang++-3.8 cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4 && make install'
                    }
                }

                stage('Ubuntu Xenial/clang-3.8.0/llvm-3.8.0 (Release)') {
                    agent { docker { image 'ubuntu:xenial' } }
                    steps {
                        sh '/bin/bash -c "source ./peloton/script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && CC=clang-3.8 CXX=clang++-3.8 cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4 && make install'
                    }
                }

                stage('Ubuntu Trusty/clang-3.7.1/llvm-3.7.1 (Debug)') {
                    agent { docker { image 'ubuntu:trusty' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && CC=clang-3.7 CXX=clang++-3.7 cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4'
                    }
                }

                stage('Ubuntu Trusty/clang-3.7.1/llvm-3.7.1 (Release)') {
                    agent { docker { image 'ubuntu:trusty' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && CC=clang-3.7 CXX=clang++-3.7 cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                    }
                }

                stage('Fedora 26/clang-4.0.1/llvm-4.0.1 (Debug)') {
                    agent { docker { image 'fedora:26' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && CC=clang CXX=clang++ cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4'
                    }
                }

                stage('Fedora 26/clang-4.0.1/llvm-4.0.1 (Release)') {
                    agent { docker { image 'fedora:26' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && CC=clang CXX=clang++ cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                    }
                }

                stage('Fedora 27/clang-4.0.1/llvm-4.0.1 (Debug)') {
                    agent { docker { image 'fedora:27' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && CC=clang CXX=clang++ cmake -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4'
                    }
                }

                stage('Fedora 27/clang-4.0.1/llvm-4.0.1 (Release)') {
                    agent { docker { image 'fedora:27' } }
                    steps {
                        sh 'sudo /bin/bash -c "source ./script/installation/packages.sh"'
                        sh 'python ./script/validators/source_validator.py'
                        sh 'mkdir build'
                        sh 'cd build && CC=clang CXX=clang++ cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4'
                    }
                }

                // Omit this configuration for now since the corresponding version of clang does not seem to be available on this platform
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
