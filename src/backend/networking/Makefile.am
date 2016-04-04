## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# PROTOBUF
######################################################################

BUILT_SOURCES += \
				 backend/networking/abstract_service.pb.cc

dist_noinst_DATA = \
				    backend/networking/abstract_service.proto

MOSTLYCLEANFILES = \
				    backend/networking/abstract_service.pb.cc \
				    backend/networking/abstract_service.pb.h

backend/networking/abstract_service.pb.cc:
	$(PROTOC) --proto_path=$(srcdir)/backend/networking \
	$(srcdir)/backend/networking/abstract_service.proto \
	--cpp_out=$(srcdir)/backend/networking

######################################################################
# NETWORKING
######################################################################

networking_FILES = \
networking_FILES = \
				backend/networking/abstract_service.pb.cc \
				backend/networking/rpc_channel.cpp \
				backend/networking/rpc_server.cpp \
				backend/networking/rpc_client.cpp \
				backend/networking/peloton_service.cpp \
				backend/networking/tcp_listener.cpp \
				backend/networking/tcp_connection.cpp \
				backend/networking/tcp_address.cpp \
				backend/networking/connection_manager.cpp

networking_INCLUDES = \
                  -I$(srcdir)/networking
