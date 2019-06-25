#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""dkredis - Python interface to Redis
"""

classifiers = """\
Development Status :: 3 - Alpha
Intended Audience :: Developers
Programming Language :: Python
Programming Language :: Python :: 2
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3
Topic :: Software Development :: Libraries
"""

import sys
import setuptools
from distutils.core import setup, Command
from setuptools.command.test import test as TestCommand

version = '0.1.1'


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


setup(
    name='dkredis',
    version=version,
    requires=[
        'redis'
    ],
    install_requires=[
        'redis',
    ],
    description=__doc__.strip(),
    classifiers=[line for line in classifiers.split('\n') if line],
    long_description=open('README.rst').read(),
    cmdclass={'test': PyTest},
    packages=['dkredis'],
    zip_safe=False,
)
