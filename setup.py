#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name='dask_k8',
      version='0.1',
      license='GPL',
      author="Benoit Seguin",
      url='https://github.com/SeguinBe/dask_k8',
      description='Simple library to start a Dask cluster on Kubernetes',
      packages=find_packages(),
      python_requires='>=3.6',
      classifiers=[
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.6',
      ],
      install_requires=[
          'distributed>=1.27',
          'requests>=2.20',
          'kubernetes>=9.0',
      ])
