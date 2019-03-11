#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages
import versioneer


with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")

with open("README.rst") as f:
    long_description = f.read()


setup(
    name="intake-stac",
    description="An intake plugin for building intake catalogs begining with SpatioTemporal Asset Catalogs (STAC)",
    long_description=long_description,
    python_requires=">3.5",
    maintainer="Joe Hamman",
    maintainer_email="jhamman@ucar.edu",
    url="https://github.com/pangeo-data/intake-stack",
    packages=find_packages(),
    package_dir={"intake-stack": "intake-stack"},
    include_package_data=True,
    install_requires=install_requires,
    license="Apache 2.0",
    zip_safe=False,
    keywords="intake-stack",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
