#!/usr/bin/env bash
# This script should be run from the repository root
jzosPath=$1

usage () {
  echo "Usage: $(basename $0) <path to IBM Java SDK>"
}

[ ! -z "$jzosPath" ] || usage

if [ ! -d "$jzosPath/lib/ext" ]; then
  echo "IBM JDK not found at $jzosPath"
  exit 1
fi

mkdir -p lib
ln -vs $jzosPath/lib/ext/ibmjzos.jar lib/ibmjzos.jar
ln -vs $jzosPath/lib/ext/ibmjcecca.jar lib/ibmjcecca.jar
