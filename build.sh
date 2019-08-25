#!/usr/bin/env bash
echo $(date +%Y%m%d%H%M) > src/main/resources/build.txt
sbt -Dsbt.log.noformat=true package
