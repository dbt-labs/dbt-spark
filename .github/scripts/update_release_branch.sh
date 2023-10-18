#!/bin/bash -e
set -e

release_branch=$1
target_req_file=".github/workflows/nightly-release.yml"
if [[ "$OSTYPE" == darwin* ]]; then
 # mac ships with a different version of sed that requires a delimiter arg
 sed -i "" "s|[0-9].[0-9].latest|$release_branch|" $target_req_file
else
 sed -i "s|[0-9].[0-9].latest|$release_branch|" $target_req_file
fi
