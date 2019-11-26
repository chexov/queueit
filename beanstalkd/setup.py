from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='queueit',
    version='1.1.1',
    author='Anton P. Linevich',
    author_email='anton@linevich.com',
    keywords="beanstalkd shell client",
    packages=['queueit', ],
    scripts=[],
    url='https://github.com/chexov/queueit',
    license='LICENSE.txt',
    description='Simple tool for integration beanstalkd queues into shell script pipelines',
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=['PyYAML', ],
    python_requires='>=3.2',
    entry_points={
        'console_scripts': [
            'q-peek = queueit:main',
            'q-peek-ready = queueit:main',
            'q-peek-delayed = queueit:main',
            'q-peek-buried = queueit:main',
            'q-get = queueit:main',
            'q-kick = queueit:main',
            'q-put = queueit:main',
            'q-stat = queueit:main',
            'q-wrapper = queueit:main',
            'q-cleanup = queueit:main'
        ]
    }
)
