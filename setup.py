#!/usr/bin/env python
from setuptools import find_namespace_packages, setup
import os
import re


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


package_name = "dbt-spark"


# get this from a separate file
def _dbt_spark_version():
    _version_path = os.path.join(
        this_directory, 'dbt', 'adapters', 'spark', '__version__.py'
    )
    _version_pattern = r'''version\s*=\s*["'](.+)["']'''
    with open(_version_path) as f:
        match = re.search(_version_pattern, f.read().strip())
        if match is None:
            raise ValueError(f'invalid version at {_version_path}')
        return match.group(1)


package_version = _dbt_spark_version()
description = """The SparkSQL plugin for dbt (data build tool)"""

dbt_version = '0.20.0rc1'
# the package version should be the dbt version, with maybe some things on the
# ends of it. (0.20.0rc1 vs 0.20.0rc1a1, 0.20.0rc1.1, ...)
if not package_version.startswith(dbt_version):
    raise ValueError(
        f'Invalid setup.py: package_version={package_version} must start with '
        f'dbt_version={dbt_version}'
    )

odbc_extras = ['pyodbc>=4.0.30']
pyhive_extras = [
    'PyHive[hive]>=0.6.0,<0.7.0',
    'thrift>=0.11.0,<0.12.0',
]
all_extras = odbc_extras + pyhive_extras

setup(
    name=package_name,
    version=package_version,

    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',

    author='Fishtown Analytics',
    author_email='info@fishtownanalytics.com',
    url='https://github.com/fishtown-analytics/dbt-spark',

    packages=find_namespace_packages(include=['dbt', 'dbt.*']),
    include_package_data=True,
    install_requires=[
        f'dbt-core=={dbt_version}',
        'sqlparams>=3.0.0',
    ],
    extras_require={
        "ODBC": odbc_extras,
        "PyHive":  pyhive_extras,
        "all": all_extras
    }
)
