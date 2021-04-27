import os
import sys


if sys.version_info[:2] < (3, 7):
    raise ImportError(
        """
    This version of helpers doesn't support python versions less than 3.7
    """
    )

#try:
#    from setuptools import setup
#except ImportError:
#    from distutils.core import setup

from setuptools import setup, find_packages

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "helpers")
)

#import version  # NOQA

#INSTALL_REQUIRES = ["pandas>=1.0.0",
#                    "requests>=2.18.0",
#                    "tqdm>=4.50.0",
#                    "halo>=0.0.31"]
INSTALL_REQUIRES = [
        "numpy >= 1.17.5",
        "pandas >= 1.1.1",
        "docker >= 4.3.1",
        "jsonpickle >= 1.4.1",
        "boto3 >= 1.14.51",
        "botocore >= 1.17.51",
        "psycopg2 >= 2.8.5",
        "matplotlib >= 3.3.1",
        "lxml >= 4.5.2",
]

TEST_REQUIRES = ["pytest>=5.0.0"]
PACKAGES = [
    "helpers",
]
#PACKAGES=find_packages(where='.'),  # Required
print(PACKAGES)
#assert 0

#project_urls = {
#    "Site": "https://particle.one/",
#    "API registration": "https://particle.one/api-access",
#}

project_urls = {}

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    #version=version.VERSION,
    version="1.1.1",
    name="helpers",
    #description="Package for P1 Data API access",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=[
    ],
    author="GP Saggese, Paul Smith",
    author_email="gp@particle.one, paul@particle.one",
    maintainer="",
    maintainer_email="",
    url="",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=INSTALL_REQUIRES,
    tests_require=TEST_REQUIRES,
    python_requires=">= 3.7",
    test_suite="pytest",
    packages=PACKAGES,
    project_urls=project_urls,
)
#import setuptools
#
#
#setuptools.setup(
#    name="helpers",
#    ext_package="helpers",
#    version="1.1.0",
#    author="GP, Paul",
#    author_email="saggese@gmail, smith.paul.anthony@gmail.com",
#    description="A package containing amp/helpers",
#    url="https://github.com/alphamatic/amp/helpers",
#    packages=setuptools.find_packages(),
#    include_package_data=True,
#    python_requires='>=3.7',
#    install_requires=[
#        "numpy >= 1.17.5",
#        "pandas >= 1.1.1",
#        "docker >= 4.3.1",
#        "jsonpickle >= 1.4.1",
#        "boto3 >= 1.14.51",
#        "botocore >= 1.17.51",
#        "psycopg2 >= 2.8.5",
#        "matplotlib >= 3.3.1",
#        "lxml >= 4.5.2",
#    ]
#)
