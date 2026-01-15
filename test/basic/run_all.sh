#!/bin/bash

cd $PROJECT_HOME/test

./run.sh basic/catalog_ddl.skt
./run.sh basic/dataset_ddl.skt
./run.sh basic/data_gen_load_dump.skt
./run.sh basic/knn.skt
./run.sh basic/ivf.skt

