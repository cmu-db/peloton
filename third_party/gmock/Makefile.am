# Build rules for testing framework.
GMOCK_DIR = third_party/gmock

GMOCK_SRCDIR = $(top_srcdir)/$(GMOCK_DIR)
GMOCK_BUILDDIR = $(top_builddir)/$(GMOCK_DIR)

noinst_LTLIBRARIES = \
					third_party/gmock/libgmock.la \
					third_party/gmock/libgmock_main.la

third_party_gmock_libgmock_la_SOURCES = \
					  third_party/gmock/gmock/gmock.h \
					  third_party/gmock/gtest/gtest.h \
					  third_party/gmock/gmock-gtest-all.cc
third_party_gmock_libgmock_la_CPPFLAGS = $(GTEST_CPPFLAGS) -isystem $(GMOCK_SRCDIR)
third_party_gmock_libgmock_la_CXXFLAGS = $(GTEST_CXXFLAGS)
third_party_gmock_libgmock_la_LDFLAGS = $(GTEST_LDFLAGS)
third_party_gmock_libgmock_la_LIBADD = $(GTEST_LIBS)

third_party_gmock_libgmock_main_la_SOURCES = \
						   third_party/gmock/gmock/gmock.h \
						   third_party/gmock/gtest/gtest.h \
						   third_party/gmock/gmock_main.cc
third_party_gmock_libgmock_main_la_CPPFLAGS = $(GTEST_CPPFLAGS) -isystem $(GMOCK_SRCDIR)
third_party_gmock_libgmock_main_la_CXXFLAGS = $(GTEST_CXXFLAGS)
third_party_gmock_libgmock_main_la_LDFLAGS = $(GTEST_LDFLAGS)
third_party_gmock_libgmock_main_la_LIBADD = \
						  $(GTEST_LIBS) \
						  third_party/gmock/libgmock.la

