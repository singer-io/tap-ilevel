#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-ilevel',
      version='1.0.0',
      description='Singer.io tap for extracting data from the ilevel 2.0 API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_ilevel'],
      install_requires=[
          'suds-jurko==0.6',
          'pyhumps==1.6.1',
          'backoff==1.8.0',
          'requests==2.23.0',
          'singer-python==5.9.0'
      ],
      extras_require={
        'dev': [
            'pylint',
            'ipdb',
            'nose'
        ]
      },
      entry_points='''
          [console_scripts]
          tap-ilevel=tap_ilevel:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_ilevel': [
              'schemas/*.json'
          ]
      })
