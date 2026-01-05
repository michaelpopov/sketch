#!/bin/bash

cd ../client
make && ./debug/client -c config.ini -i
