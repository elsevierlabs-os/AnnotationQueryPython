# How to generate the documentation

## Installation
Install python libraries (tested with python 3.6):
```
pip install sphinx
pip install sphinx_rtd_theme
```

## Update copyright year and version number
Version number is defined in `conf.py` file
Copyright year is to be updated in `conf.py` and `license.rst`

In the current package library directory (for which we want to generate the python documentation), load the package in development mode:
```
pip install -e .
```

## Generate the documentation from this directory
```
make autogen html
```

Copy the files in `_build/html` in the `docs` directory at the root of the project. 
Make sure you have an empty  file named `.nojekyll` in the `docs` directory
