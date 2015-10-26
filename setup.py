#!/usr/bin/env python

from setuptools import setup, find_packages
import sys
import os
import glob


try:
    with open("requirements.txt", "r") as f:
        install_requires = [x.strip() for x in f.readlines()]
except IOError:
    install_requires = []

try:
    with open("dependency_links.txt", "r") as f:
        dependency_links = [x.strip() for x in f.readlines()]
except IOError:
    dependency_links = []

setup(name = "hugin",
      version = "0.1",
      author = "Pontus Larsson",
      author_email = "pontus.larsson@scilifelab.se",
      description = "A system for monitoring sequencing and analysis status at SciLifeLab",
      license = "MIT",
      scripts = glob.glob('scripts/*.py'),
      install_requires = install_requires,
      dependency_links = dependency_links,
      packages=find_packages(exclude=['tests']),
      )

os.system("git rev-parse --short --verify HEAD > ~/.hugin_version")
