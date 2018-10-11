import setuptools

setuptools.setup(
      name='ppdb_nba',
      version='0.1',
      scripts=['bin/ppdb_nba'],
      description='Pre-processing database for NBA',
      url='https://github.com/naturalis/ppdb_nba',
      author='Joep Vermaat',
      author_email='joep.vermaat@naturalis.nl',
      packages=['ppdb_nba'],
      zip_safe=False,
      classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: LGPL License",
        "Operating System :: OS Independent",
      ),
)
