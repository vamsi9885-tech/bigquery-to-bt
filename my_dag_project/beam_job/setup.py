
from setuptools import setup

setup(
    name='beam_to_bigtable',
    version='0.0.1',
    install_requires=[
        'google-cloud-bigtable',
        'apache-beam[gcp]',
        'pyarrow'
    ]
)
