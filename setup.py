from pathlib import Path

from setuptools import find_packages, setup

# Check for dependencies
install_requires = []
try:
    import resens
except ImportError:
    install_requires.append("resens>=0.4.2")
try:
    try:
        import gdal
    except ImportError:
        from osgeo import gdal
except ImportError:
    install_requires.append("GDAL>=3.*")
try:
    import boto3
except ImportError:
    install_requires.append("boto3")
try:
    import tqdm
except ImportError:
    install_requires.append("tqdm")
try:
    import xmltodict
except ImportError:
    install_requires.append("xmltodict")

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
    install_requires=install_requires
)