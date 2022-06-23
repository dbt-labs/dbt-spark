#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

with open('README.md') as f:
    long_description = f.read()

package_name = "dbt-spark-livy"
# make sure this always matches dbt/adapters/dbt-spark-livy/__version__.py
package_version = "1.1.0"
description = """The dbt-spark-livy adapter plugin for Spark in Cloudera DataHub with Livy interface"""

setup(
    name=package_name,
    version=package_version,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Cloudera",
    author_email="innovation-feedback@cloudera.com",
    url="https://github.com/cloudera/dbt-impala",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core>=1.1.0",
        "pyspark",
        "sqlparams"
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: Apache Software License"
    ],
    zip_safe=False,
    python_requires=">=3.7",
)
