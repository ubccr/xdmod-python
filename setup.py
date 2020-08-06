from setuptools import setup, find_packages
setup(
    name="xdmod",
    version="0.0.2",
    description='Python driver for XDMoD',
    license='LGPLv3',
    author='Joseph P White',
    author_email='jpwhite4@buffalo.edu',
    url='https://github.com/ubccr/xdmod-python',
    zip_safe=True,
    packages=find_packages(),
    install_requires=[
        'numpy',
        'urllib',
        'csv',
        'pandas',
        'pycurl',
        'mariadb'
    ]
)
