#!/usr/bin/env python

from distutils.core import setup

setup(name='sendtools',
      version='0.1.0',
      description='Tools for composing consumers for iterators. A companion '
      'to itertools.',
      author='Bryan Cole',
      author_email='bryancole.cam@googlemail.com',
      url='http://bitbucket.org/bryancole/sendtools',
      py_modules=['sendtools'],
      classifiers=['Development Status :: 3 - Alpha',
                'Intended Audience :: Developers',
                'License :: OSI Approved :: Python Software Foundation License',
                'Natural Language :: English',
                'Operating System :: OS Independent',
                'Programming Language :: Python :: 2.6',
                'Topic :: Utilities'
            ]
     )
