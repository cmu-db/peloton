FROM debian:stretch

ADD . /peloton
# preliminary steps to allow packages.sh to run
RUN apt-get -qq update && apt-get -qq -y --no-install-recommends install python-dev lsb-release sudo software-properties-common && apt-get -qq clean

RUN /bin/bash -c "source ./peloton/script/installation/packages.sh"

RUN mkdir /peloton/build && cd /peloton/build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && cd /peloton/build && make -j4 && make install

ENV PATH=$(BUILD_DIR)/bin:$PATH
ENV LD_LIBRARY_PATH=$(BUILD_DIR)/lib:$LD_LIBRARY_PATH

EXPOSE 15721

ENTRYPOINT ["./peloton/build/bin/peloton"]
