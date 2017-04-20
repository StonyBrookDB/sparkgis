
UNAME		= $(shell uname)
OPTFLAGS	= -O2 -Wno-write-strings -Wno-deprecated-declarations
JNIINC		= -I${JAVA_HOME}/../include -I${JAVA_HOME}/../include/linux
INCBASE		= -I. -I${SATO_INC_PATH}
LIBBASE		= -L${SATO_LIB_PATH}  -lgeos -lspatialindex

CFLAGS      = $(OPTFLAGS) $(INCBASE) $(INCFLAGS)
LDFLAGS     = $(LIBBASE)

CC = g++

all: gis

# gis: native/resque.cpp native/partitionMapperJoin.cpp include/partitionMapperJoin.hpp include/resque.hpp include/hadoopgis.h include/tokenizer.h include/resquecommon.h gis.cpp
# 	$(CC) -std=c++0x -shared -fpic gis.cpp native/partitionMapperJoin.cpp native/resque.cpp -o libgis.so $(JNIINC) $(CFLAGS) $(LDFLAGS) $(OPTFLAGS)
gis: native/resque.cpp include/resque.hpp include/hadoopgis.h include/tokenizer.h include/resquecommon.h gis.cpp
	$(CC) -std=c++0x -shared -fpic gis.cpp native/resque.cpp -o libgis.so $(JNIINC) $(CFLAGS) $(LDFLAGS) $(OPTFLAGS)
#$(CC) -std=c++0x -fPIC $(JNIINC) $(INCBASE) gis.cpp native/resque.cpp -o gis $(OPTFLAGS) $(LDFLAGS)
#$(CC) -shared -o ./bin/libgis.so  gis.o -Wl,-rpath,${SATO_LIB_PATH} $(CFLAGS) $(LDFLAGS) $(OPTFLAGS)

clean:
	@rm -f *.o hgtiler mbbextractor
