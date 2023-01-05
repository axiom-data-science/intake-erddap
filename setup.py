#!/usr/bin/env python

from pathlib import Path
from setuptools import setup


requires = open("requirements.txt").read().strip().split("\n")

setup(
    name="intake-erddap",
    description="ERDDAP plugin for Intake",
    use_scm_version={
        "write_to": "intake_erddap/_version.py",
        "write_to_template": '__version__ = "{version}"',
        "tag_regex": r"^(?P<prefix>v)?(?P<version>[^\+]+)(?P<suffix>.*)?$",
    },
    setup_requires=["setuptools_scm", "setuptools_scm_git_archive"],
    url="https://github.com/axiom-data-science/intake-erddap",
    maintainer="Axiom Data Science",
    maintainer_email="dev@axds.co",
    license="BSD",
    packages=["intake_erddap"],
    package_data={"": ["*.csv", "*.yml", "*.html"]},
    entry_points={
        "intake.drivers": [
            "tabledap = intake_erddap.erddap:TableDAPSource",
            "griddap = intake_erddap.erddap:GridDAPSource",
            "erddap_cat = intake_erddap.erddap_cat:ERDDAPCatalog",
        ]
    },
    include_package_data=True,
    install_requires=requires,
    long_description=Path("README.md").read_text(),
    long_description_content_type='text/markdown',
    zip_safe=False,
)
