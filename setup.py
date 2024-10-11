from setuptools import setup, find_namespace_packages


if __name__ == '__main__':
    setup(
        name='codoscope',
        version='0.0.1',
        packages=find_namespace_packages(where='src'),
        package_dir={'': 'src'},
        install_requires=[
            'GitPython',
            'tzlocal',
            'plotly',
            'pandas',
            'coloredlogs',
            'pyyaml',
            'atlassian-python-api',
            'python-dateutil',
        ],
        entry_points={
            'console_scripts': ['codoscope=codoscope.cli:entrypoint'],
        }
    )
