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
