#!/usr/bin/env python

from setuptools import setup, find_packages
import sys
import os
import glob

setup(name = "hugin",
      version = "0.1",
      author = "Pontus Larsson",
      author_email = "pontus.larsson@scilifelab.se",
      description = "A system for monitoring sequencing and analysis status at SciLifeLab",
      license = "MIT",
      scripts = glob.glob('scripts/*.py'),
      install_requires = [
        "couchdb >= 0.8",
        "py-trello",
        "oauth2"
        ],
      packages=find_packages(exclude=['tests']),
      )

os.system("git rev-parse --short --verify HEAD > ~/.hugin_version")
