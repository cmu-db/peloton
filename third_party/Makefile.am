## Makefile.am -- Process this file with automake to produce Makefile.in

######################################################################
# THIRD PARTY
######################################################################
		  
all-local:
	-python ../../scripts/installation/nvml.py --build
	
install-exec-local:
	-python ../../scripts/installation/nvml.py --install
		  
third_party_FILES = \
			   ../third_party/jsoncpp/jsoncpp.cpp \
			   ../third_party/murmur3/MurmurHash3.cpp

third_party_INCLUDES = \
                  -I$(top_srcdir)/third_party
