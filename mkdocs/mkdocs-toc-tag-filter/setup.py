from setuptools import find_packages, setup

setup(
    name="mkdocs-toc-tag-filter",
    version="0.1.0",
    description="A MkDocs plugin to filter <!-- toc --> tag content",
    long_description="",
    keywords="mkdocs",
    url="",
    author="",
    author_email="",
    license="MIT",
    python_requires=">=3.8",
    install_requires=["mkdocs>=1.0.4"],
    classifiers=["Programming Language :: Python :: 3.8"],
    packages=find_packages(),
    entry_points={
        "mkdocs.plugins": ["mkdocs-toc-tag-filter =  mkdocs_toc_tag_filter.plugin:TocFilterPlugin"]
    },
)
