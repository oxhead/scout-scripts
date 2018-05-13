setup_options = dict(
    name='scout',
    version='0.1',
    description='Benchmark tools for generating SCOUT dataset',
    long_description=open('README.rst').read(),
    author='NCSU Operating Research Lab',
    url='https://github.com/oxhead/scout-scripts',
    scripts=['sbin/myaws_allinone', 'sbin/myhadoop_dist'],
    install_requires=[
        'boto3',
        'click',
        'executor',
    ],
    extras_require={
        ':python_version=="3.4"': [
            'click>=6.7',
        ]
    },
    license="Apache License 2.0",
    classifiers=(
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ),
)

setup(**setup_options)
