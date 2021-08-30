from setuptools import setup, find_packages
setup(
    name="beam_helper_classes",
    version="0.0.1",
    author="chrispearce",
    author_email="chrispearce10@python.com",
    description="Functions and Classes to aid with apache beam development",
    url="https://github.com/cptestingrepo101/beam_helper_classes.git",
    license="MIT",
    packages=["beam_helper_classes"],
    install_requires=["wheel", "apache_beam>=2.31.0"]
)