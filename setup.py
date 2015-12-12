#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

requirements = [
    "celery",
    "redis"
]

setup(
    name='celery_one',
    version='0.1',
    description='Allows you to prevent multiple execution and queuing of celery tasks.',
    long_description=readme,
    author='Simone Zandara',
    author_email='simone.zandara@gmail.com',
    url='https://github.com/szandara/celery-one',
    packages=find_packages(),
    install_requires=requirements,
    license="BSD",
    keywords='celery, mutex, one, id, lock, redis',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: System :: Distributed Computing'
    ],
)
