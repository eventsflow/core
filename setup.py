
import os

from codecs import open

from setuptools import setup
from setuptools import find_namespace_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md'), 'r', 'utf-8') as handle:
    readme = handle.read()


setup(
    name='eventsflow',
    version='0.1.24.dev0',
    description="Events Flow Core components",
    long_description=readme,
    url="https://github.com/eventsflow/eventsflow-core",
    author="ownport",
    author_email="ownport@gmail.com",
    packages=['eventsflow', ] + find_namespace_packages(include=["eventsflow.*", ]),
    python_requires='>=3.5',
    install_requires=[
        'jinja2==2.11.2',
        'pyyaml==5.3.1',
    ],
    entry_points={
        'console_scripts': ['eventsflow=eventsflow.cli:launch_new_instance']
    },
    classifiers = [
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: CPython',
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
    ]
)
