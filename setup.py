#!/usr/bin/env python
from setuptools import find_packages
from distutils.core import setup
import os


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md')) as f:
    long_description = f.read()


package_name = "dbt-spark"
package_version = "0.14.3"
description = """The SparkSQL plugin for dbt (data build tool)"""

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
        'dbt-core=={}'.format(package_version),
        'PyHive>=0.6.0,<0.7.0',
        'thrift>=0.11.0,<0.12.0',
    ]
)
