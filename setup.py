import sys, os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

packages = [
    'scrapy_rabbitmq_link'
]

requires = [
    'pika',
    'Scrapy>=1.3'
]

setup(
    name='scrapy-rabbitmq-link',
    author='Mantas Briliauskas',
    description='RabbitMQ plug-in for Scrapy',
    version='0.3.0',
    author_email='m.briliauskas@gmail.com',
    license='MIT',
    url='https://github.com/mbriliauskas/scrapy-rabbitmq-link',
    install_requires=requires,
    packages=packages
)
