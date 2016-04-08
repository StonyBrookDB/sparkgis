
UNAME		= $(shell uname)
OPTFLAGS	= -O2 -Wno-write-strings -Wno-deprecated-declarations
#INCBASE		= -I. -I extlib/include
#LIBBASE		= -lspatialindex -lgeos -L extlib/lib
JNIINC		= -I/cm/shared/apps/jdk1.8.0_45/include -I/cm/shared/apps/jdk1.8.0_45/include/linux
INCBASE		= -I. -I/usr/local/lib -I ${SATO_INC_PATH}
LIBBASE		= -lspatialindex -lgeos -L/usr/local/lib -L ${SATO_LIB_PATH}

ifneq (,$(findstring Linux,$(shell uname -a)))
    INCFLAGS = -I$(spatial)/include
    LIBS = -L$(spatial)/lib
else 
    ifeq ($(UNAME), Darwin)
    INCFLAGS = -I /Library/Frameworks/GEOS.framework/Headers
    LIBS = -L /Library/Frameworks/GEOS.framework/unix/lib
endif
endif

CFLAGS      = $(OPTFLAGS) $(INCBASE) $(INCFLAGS) 
LDFLAGS     = $(LIBBASE) $(LIBS)

CC = g++

all: gis

gis: native/resque.cpp native/partitionMapperJoin.cpp include/partitionMapperJoin.hpp include/resque.hpp include/hadoopgis.h include/tokenizer.h include/resquecommon.h gis.cpp
	$(CC) -std=c++0x -shared -fpic gis.cpp native/partitionMapperJoin.cpp native/resque.cpp -o libgis.so $(JNIINC) $(CFLAGS) $(LDFLAGS) $(OPTFLAGS)
clean:
	@rm -f *.o hgtiler mbbextractor
