
import os

from codecs import open

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md'), 'r', 'utf-8') as handle:
    readme = handle.read()


def build_package_names(path):

    packages = [path, ]
    for root, dirs, files in os.walk(path):
        packages.extend(['.'.join([root, d]) for d in dirs])
    return packages


setup(
    name='eventsflow',
    version='0.1.2',
    description="Events Flow",
    long_description=readme,
    packages=build_package_names('eventsflow'),
    python_requires='>=3.5',
    install_requires=[
        'jinja2==2.10.1',
        'pyyaml==5.1',
    ],
    entry_points={
        'console_scripts': ['eventsflow=eventsflow.cli:launch_new_instance']
    },
    classifiers = [
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
    ]
)
