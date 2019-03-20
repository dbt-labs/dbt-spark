#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup

package_name = "dbt-spark"
package_version = "0.13.0-rc1"
description = """The dbt_spark adpter plugin for dbt (data build tool)"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author='Drew Banin',
    author_email='drew@fishtownanalytics.com',
    url='https://github.com/fishtown-analytics/dbt-spark',
    packages=find_packages(),
    package_data={
        'dbt': [
            'include/dbt_spark/dbt_project.yml',
            'include/dbt_spark/macros/*.sql',
        ]
    },
    install_requires=[
        'dbt-core=={}'.format(package_version),
        'PyHive>=0.6.0,<0.7.0',
        'thrift>=0.11.0,<0.12.0'
    ]
)
