from setuptools import setup, find_packages

setup(
    name="ldproxy-api-scaffold",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "sqlalchemy>=1.4.0",
        "psycopg2-binary>=2.9.0",
        "PyYAML >=0.1.0",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="A configuration generator for ldproxy based on database schemas",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/ldproxy-config-generator",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)