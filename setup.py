from setuptools import setup, find_packages

with open('README.md', 'r') as fh:
    long_desc = fh.read()

setup(
    name='arbor_worker_tasks',
    version='0.0.0',
    description='Girder Worker tasks for Arbor',
    long_description=long_desc,
    author='Kitware Inc',
    author_email='kitware@kitware.com',
    license='Apache Software License 2.0',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Scientific/Engineering',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'Programming Language :: Python'
    ],
    install_requires=[
        'girder_worker',
        'girder_worker_utils',
        # TODO: Add additional packages required by both
        # producer and consumer side installations
        'pandas'
    ],
    extras_require={
        'girder': [
            # TODO: Add dependencies here that are required for the
            # package to work on the producer (Girder) side.
        ],
        'worker': [
            # TODO: Add dependencies here that are required for the
            # package to work on the consumer (Girder Worker) side.
        ]
    },
    include_package_data=True,
    entry_points={
        'girder_worker_plugins': [
            'arbor_worker_tasks = arbor_worker_tasks:ArborWorkerTasks',
        ]
    },
    packages=find_packages(),
    zip_safe=False)
