#!/bin/bash
# Auto format changed java files using google-java-format.
# This is run through a pre-commit hook which will pass in all modified
# java files to this script as command line arguments

set -e

# Grab root directory to help with creating an absolute path for changed files.
if [ "$1" == 'git' ]
then
  root_dir="$(git rev-parse --show-toplevel)"
else
  root_dir="$1"
fi
shift

[ -d "${root_dir}" ] || exit 1

# relative location of the format jar
jar_base_dir=".scripts/"

formatter_jar="${root_dir}/.scripts/google-java-format-1.15.0-all-deps.jar"

# Format file in-place and use 4-space style (AOSP).
java -jar ${formatter_jar} $@
