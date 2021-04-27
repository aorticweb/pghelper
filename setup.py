#!/usr/bin/env python

"""
Setup script for installing parseops
"""

##########################################################################
## Imports
##########################################################################

import os
import codecs

from setuptools import setup, find_packages


##########################################################################
## Package Information
##########################################################################

NAME        = "pghelper"
DESCRIPTION = "common help function for postgres"
AUTHOR      = "Brice"
EMAIL       = "nicoline@gmail.com"
MAINTAINER  = "Brice"
LICENSE     = "TBD"
REPOSITORY  = "TBD"
PACKAGE     = "pghelper"


CLASSIFIERS = [
    'License :: Other/Proprietary License',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3 :: Only',
]

PROJECT = os.path.abspath(os.path.dirname(__file__))
REQUIRE_PATH = "requirements.txt"
VERSION_PATH = os.path.join(PACKAGE, "version.py")

EXCLUDES = [
    "tests", "bin", "docs"
]


##########################################################################
## Helper Function
##########################################################################

def read(*parts):
    with codecs.open(os.path.join(PROJECT, *parts), "rb", "utf-8") as f:
        return f.read()

def get_requires(path=REQUIRE_PATH):
    for line in read(path).splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            yield line


##########################################################################
## Define the configuration
##########################################################################

config = {
    "name": NAME,
    "description": DESCRIPTION,
    "classifiers": CLASSIFIERS,
    "license": LICENSE,
    "author": AUTHOR,
    "author_email": EMAIL,
    "maintainer": MAINTAINER,
    "maintainer_email": EMAIL,
    "packages": find_packages(where=PROJECT, exclude=EXCLUDES),
    "zip_safe": True,
    "install_requires": list(get_requires()),
    "python_requires": ">=3.6,<4",
}


if __name__ == '__main__':
    setup(**config)
