#!/bin/sh

# Install neccessary packages
dnf install -y git \
	gcc-c++ \
	cmake \
	gflags-devel \
	protobuf-devel \
	bison \
	flex \
	libevent-devel \
	boost-devel \
	jemalloc-devel \
	valgrind \
	lcov \
	postgresql-contrib
