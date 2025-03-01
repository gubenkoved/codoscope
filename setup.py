from setuptools import find_namespace_packages, setup

if __name__ == "__main__":
    setup(
        name="codoscope",
        version="0.0.1",
        packages=find_namespace_packages(where="src"),
        package_dir={"": "src"},
        install_requires=[
            "GitPython",
            "tzlocal",
            "plotly",
            "pandas",
            "coloredlogs",
            "pyyaml",
            "atlassian-python-api",
            "python-dateutil",
            "wordcloud",
            "Jinja2",
            "Faker",
            "pathvalidate",
        ],
        entry_points={
            "console_scripts": ["codoscope=codoscope.cli:entrypoint"],
        },
    )
