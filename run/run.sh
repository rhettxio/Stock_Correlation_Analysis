#!/usr/bin/env bash

spark-submit --master yarn --deploy-mode client \
--packages org.postgresql:postgresql:42.2.10 \
src/correlation.py > /home/hadoop/output.txt
