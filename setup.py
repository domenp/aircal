import pathlib
from setuptools import setup, find_packages

BASE_DIR = pathlib.Path(__file__).parent
README = (BASE_DIR / 'README.md').read_text()


setup(
    name='aircal',
    version='0.1.1',
    description='Export and visualize Airflow DAG runs as events in Google calendar.',
    long_description=README,
    long_description_content_type='text/markdown',
    url='http://github.com/domenp/aircal',
    author='Domen Pogacnik',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'numpy',
        'pandas',
        'croniter',
        'google-api-python-client',
        'google-auth-httplib2',
        'google-auth-oauthlib',
        'sqlalchemy'
    ],
    tests_require=['pytest'],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Development Status :: 3 - Alpha',
    ],
    zip_safe=False)
