import setuptools

setuptools.setup(
      name='nba_percolator',
      version='0.1',
      scripts=['bin/percolator'],
      description='NBA Percolator - Pre-processing database for NBA',
      url='https://github.com/naturalis/nba_percolator',
      author='Joep Vermaat',
      author_email='joep.vermaat@naturalis.nl',
      packages=['nba_percolator'],
      zip_safe=False,
      classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: LGPL License",
        "Operating System :: OS Independent",
      ),
)
