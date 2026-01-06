#!/bin/bash

cd $PROJECT_HOME/test

./run.sh basic/catalog_ddl.skt
./run.sh basic/dataset_ddl.skt
./data_gen_load_dump.skt
./knn.skt
./ivf.skt

