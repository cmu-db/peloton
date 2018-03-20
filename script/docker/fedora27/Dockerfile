FROM fedora:27

ADD . /peloton
# preliminary steps to allow packages.sh to run
RUN dnf -q -y install sudo

RUN /bin/bash -c "source ./peloton/script/installation/packages.sh"

RUN mkdir /peloton/build && cd /peloton/build && PATH=/usr/lib64/llvm4.0/bin:$PATH cmake -DCMAKE_CXX_FLAGS="-isystem /usr/include/llvm4.0" -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4 && make install

ENV PATH=$(BUILD_DIR)/bin:$PATH
ENV LD_LIBRARY_PATH=$(BUILD_DIR)/lib:$LD_LIBRARY_PATH

EXPOSE 15721

ENTRYPOINT ["./peloton/build/bin/peloton"]
