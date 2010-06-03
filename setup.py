#!/usr/bin/env python
from distribute_setup import use_setuptools
use_setuptools()

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

#from setuptools import setup

ext_modules = [Extension("sendtools", ["sendtools.pyx"])]

setup(name='sendtools',
      version='0.1.0',
      description='Tools for composing consumers for iterators. A companion '
      'to itertools.',
      author='Bryan Cole',
      author_email='bryancole.cam@googlemail.com',
      url='http://bitbucket.org/bryancole/sendtools',
      cmdclass = {'build_ext': build_ext},
      ext_modules = ext_modules,
      classifiers=['Development Status :: 3 - Alpha',
                'Intended Audience :: Developers',
                'License :: OSI Approved :: Python Software Foundation License',
                'Natural Language :: English',
                'Operating System :: OS Independent',
                'Programming Language :: Python :: 2.6',
                'Topic :: Utilities'
            ]
     )
