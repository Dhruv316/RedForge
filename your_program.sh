#!/bin/sh
#
# Use this script to run your program LOCALLY.
#
# Note: Changing this script WILL NOT affect how CodeCrafters runs your program.
#
# Learn more: https://codecrafters.io/program-interface

set -e # Exit early if any commands fail

(
  cd "$(dirname "$0")"
  mvn -B package -DskipTests -Ddir=/tmp/codecrafters-build-redis-java
)

exec java -jar /tmp/codecrafters-build-redis-java/codecrafters-redis.jar "$@"
