#!/usr/bin/python

from distutils.core import setup

setup(name='phpunit-utils',
  version="0.1",
  packages=['phpunit', 'phpunit/dataset'],
  scripts=['scripts/phpunit-datasetcreator'])
