# -*- coding: utf-8 -*-
"""
ellis_island  Copyright (C) 2015  Steven Cutting - License: ellis_island/LICENSE
"""
from setuptools import setup, find_packages

with open("README.md") as fp:
    THE_LONG_DESCRIPTION = fp.read()


setup(
    name='ellis_island',
    version='0.1.0',
    license='GNU GPL v3+',
    description="Makes misc files usable for nlp, en masse.",
    long_description=THE_LONG_DESCRIPTION,
    classifiers=['Topic :: NLP',
                 'Topic :: Text cleaning',
                 'Intended Audience :: Developers',
                 'Intended Audience :: Data Scientists',
                 'Operating System :: GNU Linux',
                 'Development Status :: 3 - Alpha',
                 'Programming Language :: Python :: 2.7',
                 'License :: GNU GPL v3+',
                 ],
    keywords='nlp encoding text plaintext preprocessing',
    author='Steven Cutting',
    author_email='steven.e.cutting@linux.com',
    packages=find_packages(exclude=('scripts', 'tests')),
    # zip_safe=False,
    install_requires=['pathlib',
                      'pyyaml',
                      'six',
                      'sqlalchemy',
                      'arrow',
                      'smart_open',  # TODO (steven_c) phase out smart_open
                      'boto3',
                      ],
    scripts=['scripts/docker_script.py'],
    )
