#!/usr/bin/env python
from setuptools import find_packages, setup
import os


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


package_name = "dbt-spark"
package_version = "0.16.0a1"
description = """The SparkSQL plugin for dbt (data build tool)"""

# evade bumpversion with this fun trick
DBT_VERSION = (0, 16, 0)
dbt_version = '.'.join(map(str, DBT_VERSION))
# the package version should be the dbt version, with maybe some things on the
# ends of it. (0.16.0 vs 0.16.0a1, 0.16.0.1, ...)
if not package_version.startswith(dbt_version):
    raise ValueError(
        f'Invalid setup.py: package_version={package_version} must start with '
        f'dbt_version={dbt_version} (from {DBT_VERSION})'
    )


setup(
    name=package_name,
    version=package_version,

    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',

    author='Fishtown Analytics',
    author_email='info@fishtownanalytics.com',
    url='https://github.com/fishtown-analytics/dbt-spark',

    packages=find_packages(),
    package_data={
        'dbt': [
            'include/spark/dbt_project.yml',
            'include/spark/macros/*.sql',
            'include/spark/macros/**/*.sql',
        ]
    },
    install_requires=[
        f'dbt-core=={dbt_version}',
        'PyHive[hive]>=0.6.0,<0.7.0',
        'thrift>=0.11.0,<0.12.0',
    ]
)
