srcdir = src
builddir = build
CXX = g++
CXXLD = g++
DEFS = -DHAVE_CONFIG_H
CPPFLAGS =  -I/usr/include -I/usr/include
CXXFLAGS = -O0 -g -ggdb -DNVML
DEBUG_CXXFLAGS = -O0 -g -ggdb -Wall -Wextra -Werror
INCLUDES = -I$(srcdir)/mc_driver/*.h
AM_CXXFLAGS = $(DEBUG_CXXFLAGS) -std=c++11 -fPIC -fpermissive \
	-fno-strict-aliasing

SHELL = /bin/bash
LIBTOOL = $(SHELL) $(builddir)/libtool
AM_LDFLAGS = -static -pthread \
	$(builddir)/src/libpelotonpg.la
LDFLAGS =  -L/usr/lib -L/usr/lib

CXXCOMPILE = $(CXX) $(DEFS) $(INCLUDES) \
	$(CPPFLAGS) $(AM_CXXFLAGS) $(CXXFLAGS)
CXXLINK = $(LIBTOOL) --tag=CXX \
	--mode=link $(CXXLD) $(AM_CXXFLAGS) \
	$(CXXFLAGS) $(AM_LDFLAGS) $(LDFLAGS)

CPP_FILES := $(wildcard $(srcdir)/mc_driver/*.cpp)
OBJ_FILES := $(addprefix $(builddir)/src/mc_driver/,$(notdir $(CPP_FILES:.cpp=.o)))

mc_driver: $(OBJ_FILES)
	$(CXXLINK) -o $(builddir)/$@ $^

$(builddir)/src/mc_driver/%.o: $(srcdir)/mc_driver/%.cpp
	@mkdir -p $(@D)
	$(CXXCOMPILE) -c -o $@ $<

clean:
	@rm -rf $(builddir)/src/mc_driver
	@rm $(builddir)/mc_driver

stylecheck:
	clang-format-3.6 --style=file ./src/postgres/backend/postmaster/postmaster.cpp | diff ./src/postgres/backend/postmaster/postmaster.cpp -
style:
	clang-format-3.6 --style=file -i ./src/postgres/backend/postmaster/postmaster.cpp

