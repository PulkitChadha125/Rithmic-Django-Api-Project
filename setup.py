from setuptools import setup, find_packages

setup(
    name='pyrithmic',
    version='0.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        # Add dependencies here if necessary
    ],
    include_package_data=True,
    zip_safe=False
)
