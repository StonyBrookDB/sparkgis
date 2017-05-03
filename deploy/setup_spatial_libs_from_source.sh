#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"

INSTALL_PATH=$SCRIPT_DIR/../deps/

echo "export SPGIS_INC_PATH=$INSTALL_PATH/include">>~/.bashrc
echo "export SPGIS_LIB_PATH=$INSTALL_PATH/lib">>~/.bashrc
echo "LD_CONFIG_PATH=\${LD_LIBRARY_PATH}:\${SPGIS_LIB_PATH}">>~/.bashrc
echo "export LD_LIBRARY_PATH=\${LD_CONFIG_PATH}">>~/.bashrc

# setup spatial libraries - compile from source since we don't have root access
wget http://download.osgeo.org/geos/geos-3.3.9.tar.bz2
tar xvf geos-3.3.9.tar.bz2
cd geos-3.3.9
cmake -DCMAKE_INSTALL_PREFIX:PATH=$INSTALL_PATH .
make
make install
cd -

wget http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.1.tar.bz2
tar xvf spatialindex-src-1.8.1.tar.bz2
cd spatialindex-src-1.8.1
cmake -DCMAKE_INSTALL_PREFIX:PATH=$INSTALL_PATH .
make
make install
cd -
