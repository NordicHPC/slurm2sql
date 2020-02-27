import setuptools

with open('slurm2sql.py') as pyfile:
    pysource = {}
    exec(pyfile.read(), pysource)
    version = pysource['__version__']

with open("README.rst", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="slurm2sql",
    version=version,
    author="Richard Darst",
    #author_email="",
    description="Import Slurm accounting database from sacct to sqlite3 database",
    long_description=long_description,
    #long_description_content_type="text/markdown", ReST is default
    url="https://github.com/NordicHPC/slurm2sql",
    #packages=setuptools.find_packages(),
    py_modules=["slurm2sql"],
    keywords='slurm sqlite3',
    python_requires='>= 2.7, >=3.4',
    entry_points={
        'console_scripts': [
            'slurm2sql=slurm2sql:main',
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: System Administrators",
        "Topic :: Database",
        "Topic :: System :: Clustering",
        "Topic :: System :: Distributed Computing",
    ],
)
