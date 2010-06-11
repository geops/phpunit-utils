#!/usr/bin/python

from distutils.core import setup

setup(name='phpunit-utils',
  version="0.2",
  packages=['phpunit', 'phpunit/dataset'],
  scripts=['scripts/phpunit-datasetcreator'])
