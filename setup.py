# type: ignore

# TODO(gp): Use `poetry build` instead of specifying the dependencies here.

from setuptools import setup

# Helper version.
VERSION = "1.2"

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


setup(
    version=VERSION,
    name="helpers",
    description="helpers",
    long_description="",
    long_description_content_type="text/markdown",
    keywords=[],
    author="GP Saggese, Paul Smith",
    author_email="gp@alphamatic.llc, paul@alphamatic.llc",
    maintainer="",
    maintainer_email="",
    url="",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=INSTALL_REQUIRES,
    tests_require=TEST_REQUIRES,
    # TODO(gp): Switch to Python 3.9.
    python_requires=">= 3.7",
    test_suite="pytest",
    packages=PACKAGES,
    project_urls={},
)
