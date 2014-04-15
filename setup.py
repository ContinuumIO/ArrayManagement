# needs to be tested
from __future__ import print_function
import sys
if len(sys.argv)>1 and sys.argv[1] == 'develop':
    # Only import setuptools if we have to
    import site
    from os.path import dirname, abspath, join
    site_packages = site.getsitepackages()[0]
    fname = join(site_packages, "arraymanagement.pth")
    path = abspath(dirname(__file__))
    with open(fname, "w+") as f:
        f.write(path)
    print("develop mode, wrote path (%s) to (%s)" % (path, fname))
    sys.exit()
from distutils.core import setup
import os
import sys
__version__ = (0, 2)
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
