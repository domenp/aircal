from setuptools import setup

setup(
    name='aircal',
    version='0.1',
    description='Export future airflow DAG runs to (G) calendar',
    url='http://github.com/domenp/aircal',
    author='Domen Pogacnik',
    license='MIT',
    packages=['aircal'],
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