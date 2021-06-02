# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE.md') as f:
    license = f.read()

setup(
    name='AnnotationQueryPython-spark3',
    version='1.0.4',
    description='Python implementation for AnnotationQuery',
    long_description=readme,
    author='Darin McBeath',
    author_email='d.mcbeath@elsevier.com',
    url='https://github.com/elsevierlabs-os/AnnotationQueryPython',
    license=license,
    packages=find_packages(exclude=('tests'))
)
