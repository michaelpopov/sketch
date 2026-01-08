#!/bin/bash

make && cd ../tester && make && ./debug/tester -c config.ini -i
