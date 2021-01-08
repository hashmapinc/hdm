import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="hashmap-data-migrator",  # Replace with your own username
    version="0.0.63.2",
    author="Hashmap, Inc",
    author_email="accelerators@hashmapinc.com",
    description="Early version of library - do not use",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hashmapinc/ctso/accelerators/data-engineering/hashmap_data_migrator",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 2 - Pre-Alpha",
    ],
    python_requires='>=3.6',
)
