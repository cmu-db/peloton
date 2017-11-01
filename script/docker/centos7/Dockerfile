FROM centos:7

ADD . /peloton
# preliminary steps to allow packages.sh to run
RUN yum -q -y install sudo wget

RUN /bin/bash -c "source ./peloton/script/installation/packages.sh"

RUN mkdir /peloton/build && cd /peloton/build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALLS=False .. && make -j4 && make install

ENV PATH=$(BUILD_DIR)/bin:$PATH
ENV LD_LIBRARY_PATH=$(BUILD_DIR)/lib:$LD_LIBRARY_PATH

EXPOSE 15721

ENTRYPOINT ["./peloton/build/bin/peloton"]
