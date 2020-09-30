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

import setuptools

version = '0.1.5'

setuptools.setup(
    name='dkredis',
    version=version,
    install_requires=[
        'redis==3.5.3',
    ],
    description=__doc__.strip(),
    classifiers=[line for line in classifiers.split('\n') if line],
    long_description=open('README.rst').read(),
    packages=['dkredis'],
    zip_safe=False,
)
