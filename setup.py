try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
 'description': 'nova-mq is a proof of concept zeromq rpc system for Openstack Nova',
 'author': 'Cloudscaling',
 'url': 'http://pypi.python.org/pypi/nova-mq',
 'download_url': 'http://pypi.python.org/pypi/nova-mq',
 'author_email': 'support@cloudscaling.com',
 'version': '0.1',
 'install_requires': ['mock', 'nose', 'pyzmq'],
 'packages': ['novamq'],
 'scripts': 'bin/novamq',
 'name': 'nova-mq'
}
setup(**config)

