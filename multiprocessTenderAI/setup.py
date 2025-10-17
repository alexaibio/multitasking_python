import setuptools


setuptools.setup(
    name='multiproc',
    version='0.0.1',
    packages=setuptools.find_packages(),
    package_data={
        '': ['conf/*']
    }
)