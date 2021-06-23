#!/usr/bin/env python

"""The setup script."""

import sys

from setuptools import find_packages, setup

with open('requirements.txt') as f:
    INSTALL_REQUIRES = f.read().strip().split('\n')

with open('README.md') as f:
    LONG_DESCRIPTION = f.read()

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
PYTHON_REQUIRES = '>=3.6'
SETUP_REQUIRES = ['setuptools_scm']
if needs_pytest:
    SETUP_REQUIRES.appen('pytest-runner >= 4.2')

TESTS_REQUIRE = ['pytest >= 2.7.1']
ENTRY_POINTS = {
    'intake.drivers': [
        'rioxarray = intake_stac.drivers:RioxarraySource',
        'stac_catalog = intake_stac.catalog:StacCatalog',
        'stac_collection = intake_stac.catalog:StacCollection',
        'stac_item_collection = intake_stac.catalog:StacItemCollection',
        'stac_item = intake_stac.catalog:StacItem',
    ]
}

description = (
    'An intake adapter for building intake catalogs begining '
    'with SpatioTemporal Asset Catalogs (STAC)'
)
setup(
    name='intake_stac',
    description=description,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    maintainer='Joe Hamman',
    maintainer_email='jhamman@ucar.edu',
    url='https://github.com/pangeo-data/intake-stac',
    py_modules=['intake_stac'],
    packages=find_packages(exclude=['*tests']),
    package_dir={'intake_stac': 'intake_stac'},
    include_package_data=True,
    python_requires=PYTHON_REQUIRES,
    install_requires=INSTALL_REQUIRES,
    setup_requires=SETUP_REQUIRES,
    tests_require=TESTS_REQUIRE,
    entry_points=ENTRY_POINTS,
    license='BSD 2-Clause',
    zip_safe=False,
    keywords='intake stac',
    use_scm_version={'version_scheme': 'post-release', 'local_scheme': 'dirty-tag'},
)
