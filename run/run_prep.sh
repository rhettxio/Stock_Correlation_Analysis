#!/usr/bin/env bash

spark-submit --master yarn --deploy-mode client \
--packages org.postgresql:postgresql:42.2.10 \
src/data_etl.py > /home/hadoop/output0.txt
