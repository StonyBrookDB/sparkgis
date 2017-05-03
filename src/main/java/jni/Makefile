
UNAME		= $(shell uname)
OPTFLAGS	= -O2 -Wno-write-strings -Wno-deprecated-declarations -Wall
JNIINC		= -I${JAVA_HOME}/../include -I${JAVA_HOME}/../include/linux
INCBASE		= -I. -I${SPGIS_INC_PATH}
LIBBASE		= -L${SPGIS_LIB_PATH}  -lgeos -lspatialindex

INCFLAGS	= $(JNIINC) $(INCBASE)
CFLAGS		= -fpic -shared
LDFLAGS     = $(LIBBASE)

CC = g++

all: gis

# gis: native/resque.cpp native/partitionMapperJoin.cpp include/partitionMapperJoin.hpp include/resque.hpp include/hadoopgis.h include/tokenizer.h include/resquecommon.h gis.cpp
# 	$(CC) -std=c++0x -shared -fpic gis.cpp native/partitionMapperJoin.cpp native/resque.cpp -o libgis.so $(JNIINC) $(CFLAGS) $(LDFLAGS) $(OPTFLAGS)
gis: native/resque.cpp include/resque.hpp include/gis.hpp gis.cpp
	$(CC) -std=c++0x $(CFLAGS) gis.cpp native/resque.cpp -o libgis.so $(INCFLAGS) $(OPTFLAGS) $(LDFLAGS)
clean:
	@rm -f *.o hgtiler mbbextractor
