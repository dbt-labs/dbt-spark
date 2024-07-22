#!/bin/bash -e
set -e


adapters_git_branch=$1
core_git_branch=$2
target_req_file="dev-requirements.txt"
core_req_sed_pattern="s|dbt-core.git.*#egg=dbt-core|dbt-core.git@${core_git_branch}#egg=dbt-core|g"
adapters_req_sed_pattern="s|dbt-adapters.git.*#egg=dbt-adapters|dbt-adapters.git@${adapters_git_branch}#egg=dbt-adapters|g"
if [[ "$OSTYPE" == darwin* ]]; then
 # mac ships with a different version of sed that requires a delimiter arg
 sed -i "" "$core_req_sed_pattern" $target_req_file
 sed -i "" "$adapters_req_sed_pattern" $target_req_file
else
 sed -i "$core_req_sed_pattern" $target_req_file
 sed -i "$adapters_req_sed_pattern" $target_req_file
fi
