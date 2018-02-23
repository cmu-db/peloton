# Dockerfile specifically for build testing from Jenkins
FROM fedora:27

ADD . /peloton
RUN dnf -q -y install sudo clang

RUN /bin/bash -c "source ./peloton/script/installation/packages.sh"

RUN echo -n "Peloton Debug build with "; g++ --version | head -1
RUN mkdir /peloton/build && cd /peloton/build && PATH=/usr/lib64/llvm4.0/bin:$PATH cmake -DCMAKE_CXX_FLAGS="-isystem /usr/include/llvm4.0" -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4 && make install

RUN echo -n "Peloton Release build with "; g++ --version | head -1
RUN rm -rf /peloton/build && mkdir /peloton/build && cd /peloton/build && PATH=/usr/lib64/llvm4.0/bin:$PATH cmake -DCMAKE_CXX_FLAGS="-isystem /usr/include/llvm4.0" -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4 && make install

RUN echo -n "Peloton Debug build with "; clang++ --version | head -1
RUN rm -rf /peloton/build && mkdir /peloton/build && cd /peloton/build && PATH=/usr/lib64/llvm4.0/bin:$PATH CC=clang CXX=clang++ cmake -DCMAKE_CXX_FLAGS="-isystem /usr/include/llvm4.0" -DCMAKE_BUILD_TYPE=Debug -DCOVERALLS=False .. && make -j4 && make install

RUN echo -n "Peloton Release build with "; clang++ --version | head -1
RUN rm -rf /peloton/build && mkdir /peloton/build && cd /peloton/build && PATH=/usr/lib64/llvm4.0/bin:$PATH CC=clang CXX=clang++ cmake -DCMAKE_CXX_FLAGS="-isystem /usr/include/llvm4.0" -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4 && make install
