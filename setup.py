from setuptools import setup, find_packages

setup(
    name="DynaBridge",
    version="0.0.5",
    packages=find_packages(),
    author='Kushagra Indurkhya',
    author_email='kushagraindurkhya7@gmail.com',
    install_requires=[
        "boto3",  # Add other dependencies here
    ],
    url='https://github.com/KushagraIndurkhya/DynaBridge',
    description="DynaBridge: An elegant DynamoDB Object-Relational Mapping (ORM) library for Python, simplifying database interactions and data modeling."
)
