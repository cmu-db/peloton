FROM ubuntu:16.04

ADD . /peloton
RUN apt-get update && apt-get -y install python-dev

RUN /bin/bash -c "source ./peloton/script/installation/packages.sh"

RUN mkdir /peloton/build

RUN cd /peloton/build && cmake -DCMAKE_BUILD_TYPE=Release -DCOVERALL=False ..

RUN cd /peloton/build && make -j4 && make install

ENV PATH=$(BUILD_DIR)/bin:$PATH
ENV LD_LIBRARY_PATH=$(BUILD_DIR)/lib:$LD_LIBRARY_PATH

EXPOSE 15721

ENTRYPOINT ["./peloton/build/bin/peloton"]
