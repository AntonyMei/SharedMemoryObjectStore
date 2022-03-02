import setuptools

# usage
# python setup.py sdist bdist_wheel
# pip install -e .
# twine check dist/*
# twine upload dist/*

# readme
with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="SMOS_antony",  # This is the name of the package
    version="1.3.3",  # major(incompatible API change).minor(new functions).patch(bug fix)
    author="Yixuan Mei",  # Full name of the author
    author_email="meiyx19@mails.tsinghua.edu.cn",
    url="https://github.com/AntonyMei/SharedMemoryObjectStore",
    description="A fast shared memory object store.",
    long_description=long_description,  # Long description read from the the readme file
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),  # List of all python modules to be installed
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],  # Information to filter the project on PyPi website
    python_requires='>=3.8',  # Minimum version requirement of the package
    py_modules=["SMOS", "SMOS_client", "SMOS_constants", "SMOS_data_track", "SMOS_exceptions", "SMOS_server",
                "SMOS_shared_memory_object", "SMOS_shared_memory_object_store", "SMOS_utils"],
    # Name of the python package
    package_dir={'': 'src'},  # Directory of the source code of the package
    install_requires=["numpy"]  # Install other dependencies if any
)
