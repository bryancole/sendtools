#!/usr/bin/env python3

from setuptools import setup
from Cython.Build import cythonize

#from setuptools import setup

#ext_modules = [Extension("sendtools", ["sendtools.pyx"])]
ext_modules = cythonize("sendtools.pyx")

setup(name='sendtools',
      version='1.0',
      description='Tools for composing consumers for iterators. A companion '
      'to itertools.',
      long_description = open("README.rst").read(),
      author='Bryan Cole',
      author_email='bryancole.cam@gmail.com',
      url='http://bitbucket.org/bryancole/sendtools',
      ext_modules = ext_modules,
      classifiers=['Development Status :: 3 - Alpha',
                'Intended Audience :: Developers',
                'License :: OSI Approved :: Python Software Foundation License',
                'Natural Language :: English',
                'Operating System :: OS Independent',
                'Programming Language :: Python :: 3.4',
                'Topic :: Utilities'
            ]
     )
