# How to generate the documentation

## Installation
Install python libraries:
```
pip install sphinx
pip install sphinx_rtd_theme
```

In the current package library directory (for which we want to generate the documentation), load the package in development mode:
```
pip install -e .
```

## Generate the documentation
```
make autogen html
```

## Update copyright year and version number
Version number is defined in `conf.py` file
Copyright year is to be updated in `conf.py` and `license.rst`