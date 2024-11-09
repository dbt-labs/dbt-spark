#!/bin/bash -e
set -e


dbt_adapters_branch=$1
dbt_core_branch=$2
dbt_common_branch=$3
target_req_file="dev-requirements.txt"
core_req_sed_pattern="s|dbt-core.git.*#egg=dbt-core|dbt-core.git@${dbt_core_branch}#egg=dbt-core|g"
adapters_req_sed_pattern="s|dbt-adapters.git|dbt-adapters.git@${dbt_adapters_branch}|g"
common_req_sed_pattern="s|dbt-common.git|dbt-common.git@${dbt_common_branch}|g"
if [[ "$OSTYPE" == darwin* ]]; then
 # mac ships with a different version of sed that requires a delimiter arg
 sed -i "" "$adapters_req_sed_pattern" $target_req_file
 sed -i "" "$core_req_sed_pattern" $target_req_file
 sed -i "" "$common_req_sed_pattern" $target_req_file
else
 sed -i "$adapters_req_sed_pattern" $target_req_file
 sed -i "$core_req_sed_pattern" $target_req_file
 sed -i "$common_req_sed_pattern" $target_req_file
fi
