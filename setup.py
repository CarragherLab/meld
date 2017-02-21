from setuptools import setup

setup(name="meld",
      version="0.0.1",
      url="http://github.com/swarchal/meld",
      description="collating cellprofiler data from eddie jobs",
      license="MIT",
      packages=["meld"],
      tests_require=["pytest"],
      install_requires=["sqlalchemy>=1.0", "pandas>=0.16", "tqdm>=4.0",
                        "numpy>=1.10"])

