from pathlib import Path

from setuptools import find_packages, setup

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="s2awsdl",
    version="0.1.0",
    description="Sentinel-2 AWS Downloader",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://www.nargyrop.com",
    author="Nikos Argyropoulos",
    author_email="n.argiropeo@gmail.com",
    license="MIT",
    packages=find_packages("s2awsdl"),
    package_dir={"": "s2awsdl"},
    python_requires=">=3.8",
    zip_safe=False,
)