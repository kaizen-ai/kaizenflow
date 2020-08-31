import setuptools


setuptools.setup(
    name="particle-helpers",
    ext_package="particle_helpers",
    version="1.0.0",
    author="Konstantin Naumov",
    author_email="kostya@particle.one",
    description="A corporate helper package",
    url="https://github.com/alphamatic/amp/helpers",
    packages=setuptools.find_packages(),
    include_package_data=True,
    python_requires='>=3.6',
    install_requires=[
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
)
