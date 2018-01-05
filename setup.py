#import ez_setup
#ez_setup.use_setuptools()
from setuptools import setup, find_packages
setup(name='digirepoq',
      version='0.1',
      packages= find_packages(),
      install_requires=[
          'celery==3.1.22',
          'bagit==1.5.4',
          'pymongo==3.2.1',
          'requests==2.13.0',
          'pillow==3.3.1',
          'PyYAML=3.12',
          'pycurl==7.43.0',
          'boto3',
      ],
)
