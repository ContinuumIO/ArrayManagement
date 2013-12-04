# needs to be tested
import sys
if len(sys.argv)>1 and sys.argv[1] == 'develop':
    # Only import setuptools if we have to
    import setuptools
from distutils.core import setup
import os
import sys
__version__ = (0, 0, 1)
setup(
    name = 'arraymanagement',
    version = '.'.join([str(x) for x in __version__]),
    packages = ['arraymanagement',
                'arraymanagement.nodes',
                ],
    author = 'Continuum Analytics',
    author_email = 'info@continuum.io',
    url = 'http://github.com/ContinuumIO/ArrayManagement',
    description = 'Array Management',
    zip_safe=False,
    license = 'New BSD',
)
