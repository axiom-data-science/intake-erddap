#!/usr/bin/env python

from setuptools import find_packages, setup


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
            "erddap = intake_erddap.intake_erddap:ERDDAPSource",
            "erddap_cat = intake_erddap.erddap_cat:ERDDAPCatalog",
            "erddap_auto = intake_erddap.intake_erddap:ERDDAPSourceAutoPartition",
            "erddap_manual = intake_erddap.intake_erddap:ERDDAPSourceManualPartition",
        ]
    },
    include_package_data=True,
    install_requires=requires,
    long_description=open("README.md").read(),
    zip_safe=False,
)
