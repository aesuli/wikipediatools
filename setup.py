import glob
import os

from setuptools import setup


setup(
    name='Wikipedia processing tools',
    version='1.0.0',
    author='Andrea Esuli <andrea.esuli@isti.cnr.it>',  # put names and emails of all authors
    author_email='andrea.esuli@isti.cnr.it',  # contact email
    scripts=glob.glob(os.path.join('bin', '*.py')),
    url='http://www.esuli.it',
    license='LICENSE.txt',
    description='Wikipedia processing tools.',
    long_description=open('README.md',encoding='utf8').read(),
    install_requires=[
    ],
)