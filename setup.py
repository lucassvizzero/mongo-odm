# coding: utf-8

from setuptools import setup, find_packages

setup(
    name='odm',
    version='0.0.2',
    description='Engine MongoDB',
    author='Lucas Ribeiro',
    author_email='lucas.ribeiro@gruponewway.com.br',
    python_requires='>=3.6',
    classifiers=['Programming Language :: Python :: 3.6'],
    packages=find_packages(),
    url='https://bitbucket.org/newway-ondemand/skjold',
    install_requires=['pymongo>=3.7.0']
)
