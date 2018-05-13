from setuptools import setup

setup(
    name='scout-sbin',
    version='0.1',
    py_modules=[
        'mysar',
        'myhibench',
        'myhadoop',
        'myspark',
    ],
    include_package_data=True,
    install_requires=[
        'click',
        'executor',
        'boto3',
    ],
    entry_points={
        'console_scripts': [
            'mysar=mysar:cli',
            'myhibench=myhibench:cli',
            'myhibench_dist=myhibench_dist:cli',
            'myhadoop=myhadoop:cli',
            'myhadoop_dist=myhadoop_dist:cli',
            'myspark=myspark:cli',
            'mysparkperf=mysparkperf:cli',
            'mysparkperf_dist=mysparkperf_dist:cli',
            'myaws=myaws:cli',
            'myaws_dist=myaws_dist:cli',
        ]
    }
)
