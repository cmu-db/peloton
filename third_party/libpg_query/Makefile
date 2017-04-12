root_dir := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

TARGET = pg_query
ARLIB = lib$(TARGET).a
PGDIR = $(root_dir)/tmp/postgres
PGDIRBZ2 = $(root_dir)/tmp/postgres.tar.bz2

PG_VERSION = 9.5.3

SRC_FILES := $(wildcard src/*.c src/postgres/*.c)
OBJ_FILES := $(SRC_FILES:.c=.o)
NOT_OBJ_FILES := src/pg_query_fingerprint_defs.o src/pg_query_fingerprint_conds.o src/pg_query_json_defs.o src/pg_query_json_conds.o src/postgres/guc-file.o src/postgres/scan.o src/pg_query_json_helper.o
OBJ_FILES := $(filter-out $(NOT_OBJ_FILES), $(OBJ_FILES))

CFLAGS  = -I. -I./src/postgres/include -Wall -Wno-unused-function -Wno-unused-value -Wno-unused-variable -fno-strict-aliasing -fwrapv -fPIC
LIBPATH = -L.

PG_CONFIGURE_FLAGS = -q --without-readline --without-zlib
PG_CFLAGS = -fPIC

ifeq ($(DEBUG),1)
	CFLAGS += -O0 -g
	PG_CONFIGURE_FLAGS += --enable-cassert --enable-debug
else
	CFLAGS += -O3 -g
	PG_CFLAGS += -O3
endif

CLEANLIBS = $(ARLIB)
CLEANOBJS = $(OBJ_FILES)
CLEANFILES = $(PGDIRBZ2)

AR = ar rs
RM = rm -f
ECHO = echo

CC ?= cc

all: build

build: $(ARLIB)

clean:
	-@ $(RM) $(CLEANLIBS) $(CLEANOBJS) $(CLEANFILES) $(EXAMPLES) $(TESTS)
	-@ $(RM) -rf {test,examples}/*.dSYM
	-@ $(RM) -r $(PGDIR) $(PGDIRBZ2)

.PHONY: all clean build extract_source examples test

$(PGDIR):
	curl -o $(PGDIRBZ2) https://ftp.postgresql.org/pub/source/v$(PG_VERSION)/postgresql-$(PG_VERSION).tar.bz2
	tar -xjf $(PGDIRBZ2)
	mv $(root_dir)/postgresql-$(PG_VERSION) $(PGDIR)
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/01_parse_replacement_char.patch
	cd $(PGDIR); patch -p1 < $(root_dir)/patches/02_normalize_alter_role_password.patch
	cd $(PGDIR); CFLAGS="$(PG_CFLAGS)" ./configure $(PG_CONFIGURE_FLAGS)
	cd $(PGDIR); make -C src/port pg_config_paths.h
	cd $(PGDIR); make -C src/backend parser-recursive # Triggers copying of includes to where they belong, as well as generating gram.c/scan.c

extract_source: $(PGDIR)
	-@ $(RM) -rf ./src/postgres/
	mkdir ./src/postgres
	mkdir ./src/postgres/include
	ruby ./scripts/extract_source.rb $(PGDIR)/ ./src/postgres/
	cp $(PGDIR)/src/include/storage/dsm_impl.h ./src/postgres/include/storage
	touch ./src/postgres/guc-file.c
	# This causes compatibility problems on some Linux distros, with "xlocale.h" not being available
	echo "#undef HAVE_LOCALE_T" >> ./src/postgres/include/pg_config.h
	echo "#undef LOCALE_T_IN_XLOCALE" >> ./src/postgres/include/pg_config.h
	echo "#undef WCSTOMBS_L_IN_XLOCALE" >> ./src/postgres/include/pg_config.h

.c.o:
	@$(ECHO) compiling $(<)
	@$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ -c $<

$(ARLIB): $(OBJ_FILES) Makefile
	@$(AR) $@ $(OBJ_FILES)

EXAMPLES = examples/simple examples/normalize examples/simple_error examples/normalize_error examples/simple_plpgsql
examples: $(EXAMPLES)
	examples/simple
	examples/normalize
	examples/simple_error
	examples/normalize_error
	examples/simple_plpgsql

examples/simple: examples/simple.c $(ARLIB)
	$(CC) -I. -o $@ -g examples/simple.c $(ARLIB)

examples/normalize: examples/normalize.c $(ARLIB)
	$(CC) -I. -o $@ -g examples/normalize.c $(ARLIB)

examples/simple_error: examples/simple_error.c $(ARLIB)
	$(CC) -I. -o $@ -g examples/simple_error.c $(ARLIB)

examples/normalize_error: examples/normalize_error.c $(ARLIB)
	$(CC) -I. -o $@ -g examples/normalize_error.c $(ARLIB)

examples/simple_plpgsql: examples/simple_plpgsql.c $(ARLIB)
	$(CC) -I. -o $@ -g examples/simple_plpgsql.c $(ARLIB)

TESTS = test/complex test/concurrency test/fingerprint test/normalize test/parse test/parse_plpgsql
test: $(TESTS)
	test/complex
	test/concurrency
	test/fingerprint
	test/normalize
	test/parse
	# Output-based tests
	test/parse_plpgsql
	diff -Naur test/plpgsql_samples.expected.json test/plpgsql_samples.actual.json

test/complex: test/complex.c $(ARLIB)
	$(CC) -I. -Isrc -o $@ -g test/complex.c $(ARLIB)

test/concurrency: test/concurrency.c test/parse_tests.c $(ARLIB)
	$(CC) -I. -o $@ -pthread -g test/concurrency.c $(ARLIB)

test/fingerprint: test/fingerprint.c test/fingerprint_tests.c $(ARLIB)
	$(CC) -I. -Isrc -o $@ -g test/fingerprint.c $(ARLIB)

test/normalize: test/normalize.c test/normalize_tests.c $(ARLIB)
	$(CC) -I. -Isrc -o $@ -g test/normalize.c $(ARLIB)

test/parse: test/parse.c test/parse_tests.c $(ARLIB)
	$(CC) -I. -o $@ -g test/parse.c $(ARLIB)

test/parse_plpgsql: test/parse_plpgsql.c $(ARLIB)
	$(CC) -I. -o $@ -I./src -I./src/postgres/include -g test/parse_plpgsql.c $(ARLIB)
