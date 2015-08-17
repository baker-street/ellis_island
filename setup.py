# -*- coding: utf-8 -*-
"""
ellis_island  Copyright (C) 2015  Steven Cutting - License: ellis_island/LICENSE
"""
from setuptools import setup, find_packages

THE_LONG_DESCRIPTION = """
Makes misc files usable for nlp, en masse.
"""

PACKAGES = ['ellis_island',
            'ellis_island.utils',
            ]

setup(
    name='ellis_island',
    version='0.1.0',
    license='GNU GPL v3+',
    description="Makes misc files usable for nlp, en masse.",
    long_description=THE_LONG_DESCRIPTION,
    classifiers=['Topic :: NLP',
                 'Topic :: Text cleaning',
                 'Intended Audience :: Developers',
                 'Operating System :: GNU Linux',
                 'Development Status :: 3 - Alpha',
                 'Programming Language :: Python :: 2.7',
                 'License :: MIT License',
                 ],
    keywords='nlp encoding text plaintext preprocessing',
    author='Steven Cutting',
    author_email='steven.e.cutting@linux.com',
    packages=PACKAGES,
    # zip_safe=False,
    install_requires=['pathlib',
                      'pyyaml',
                      'six',
                      'sqlalchemy',
                      'arrow',
                      'smart_open',  # TODO (steven_c) phase out smart_open
                      'boto3',
                      ],
    scripts=['scripts/docker_script.py']
)
"""
from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension

'''setup(ext_modules=cythonize("ellis_island/utils/misc.pyx"),
      ext_modules=cythonize("ellis_island/utils/parallel_easy.pyx"),
      ext_modules=cythonize("ellis_island/utils/threading_easy.pyx"),
      ext_modules=cythonize("ellis_island/fullprep.pyx"),
      ext_modules=cythonize("ellis_island/prepforstash.pyx"),
      ext_modules=cythonize("ellis_island/registrar.pyx"),
      ext_modules=cythonize("ellis_island/stach.pyx"),
      ext_modules=cythonize("ellis_island/stashtodatabase.pyx"),
      ext_modules=cythonize("ellis_island/stashtofile.pyx"))
'''

extensions = [Extension('*', ['ellis_island/registrar.pyx'],
                        include_dirs=['ellis_island', 'ellis_island/utils'],
                        # libraries=['gentrify'],
                        # library_dirs=['/home/steven_c/projects/gentrify/gentrify']
                        ),
              Extension('*', ['ellis_island/*.pyx'],
                        include_dirs=['ellis_island', 'ellis_island/utils'],
                        # libraries=['gentrify'],
                        # library_dirs=['/home/steven_c/projects/gentrify/gentrify/']
                        ),
              Extension('*', ['ellis_island/utils/*.pyx'],
                        include_dirs=['ellis_island', 'ellis_island/utils'],
                        # libraries=[...],
                        # library_dirs=[...])
                        )]
setup(name="ellis_island",
      ext_modules=cythonize(extensions),
      )
"""
