# coding: utf-8

from setuptools import setup, find_packages

setup(
    name='odm',
    version='0.1.1',
    description='Engine MongoDB',
    author='Grupo New Way',
    author_email='contato@gruponewway.com.br',
    python_requires='>=3.6',
    classifiers=['Programming Language :: Python :: 3.6'],
    packages=find_packages(),
    url='https://git.newwaycorp.io/libraries/python/mongo-odm',
    install_requires=[
        'pymongo>=3.7.0',
        'jsonschema>=2.6.0'
    ]
)
