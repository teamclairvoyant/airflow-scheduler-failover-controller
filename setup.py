from setuptools import setup, find_packages
from scheduler_failover_controller import __version__


setup(
    name='scheduler_failover_controller',
    description='A process that runs in unison with Apache Airflow to control the Scheduler process to ensure High Availability',
    version=__version__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    scripts=['scheduler_failover_controller/bin/scheduler_failover_controller'],
    install_requires=[
        'kazoo>=2.2.1',
        'coverage>=4.2',
        'eventlet>=0.9.7',
    ],
    extras_require={},
    author='Robert Sanders',
    author_email='robert.sanders@clairvoyantsoft.com',
    url='https://github.com/teamclairvoyant/airflow-scheduler-failover-controller',
    download_url=('https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/tarball/' + __version__)
)
