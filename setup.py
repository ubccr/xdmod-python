from setuptools import setup, find_packages
from xdmod.__version__ import __title__, __version__, __description__

setup(
    name=__title__,
    version=__version__,
    description=__description__,
    license='LGPLv3',
    author='Joseph P White',
    author_email='jpwhite4@buffalo.edu',
    url='https://github.com/ubccr/xdmod-python',
    zip_safe=True,
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'plotly',
        'requests',
    ]
)
