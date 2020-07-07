"""
Setup script for installing the "helpers" package.

Build a source archive:
> python3 setup.py sdist

Install the helpers from a source archive:
> pip install dist/helpers-1.0.tar.gz

TODO(Greg): Add changelog and other needed files for the package.
"""

from setuptools import setup

setup(
    name="helpers",
    version="1.0",
    packages=["helpers"],
    author="infra",
    author_email="infra@particle.one",
    description="A helpers package.",
    python_requires='>=3.6',
    # TODO(Greg): Add the requirements.
    install_requires=[
       #"pandas >= 0.24.1",
   ],
)
