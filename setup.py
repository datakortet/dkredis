#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""dkredis - Python interface to Redis
"""

classifiers = """\
Development Status :: 5 - Production/Stable
Intended Audience :: Developers
Programming Language :: Python
Programming Language :: Python :: 3
Topic :: Software Development :: Libraries
"""

import setuptools

version = '1.0.1'

setuptools.setup(
    name='dkredis',
    keywords='redis',
    version=version,
    url='https://github.com/datakortet/dkredis',
    install_requires=[
        'redis==5.0.1',
    ],
    description=__doc__.strip(),
    classifiers=[line for line in classifiers.split('\n') if line],
    long_description=open('README.rst').read(),
    license='MIT',
    author='bjorn',
    author_email='bp@datakortet.no',
    packages=['dkredis'],
    zip_safe=False,
)
