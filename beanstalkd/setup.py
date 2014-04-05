from setuptools import setup

setup(
    name="queueit",
    version="1.0",
    author="Anton P. Linevich",

    packages=['queueit'],

    entry_points={
        'console_scripts': [
            'q-get = queueit:main',
            'q-kick = queueit:main',
            'q-put = queueit:main',
            'q-stat = queueit:main',
            'q-wrapper = queueit:main'
        ]
    }
)
