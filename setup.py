from setuptools import setup, find_packages

setup(
    name='gnocchi-senml-proxy',
    version='0.1',
    author='Lars Kellogg-Stedman',
    author_email='lars@oddbit.com',
    url='https://github.com/larsks/gnocchi-senml-proxy',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'senml-proxy = senml_proxy.main:main',
        ],
    }
)
