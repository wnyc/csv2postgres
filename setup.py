#!/usr/bin/env python

"""
csv2postgres
============

Intelligently builds and populates SQL schemas from csv files.

"""

from setuptools import setup

setup(
    name="csv2postgres",
    version="0.0.0",
    author="Adam DePrince",
    author_email="adeprince@nypublicradio.org",
    description="",
    long_description=__doc__,
    py_modules = [
        "csv2postgres/__init__",
    ],
    zip_safe = True,
    include_package_data = True,
    classifiers = [],
    scripts = [
        "scripts/csv2postgres",
    ],
    install_requires = [
        'psycopg2',
        'python-gflags',
    ]
)
