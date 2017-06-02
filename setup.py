import sys

import setuptools
from setuptools.command.test import test as TestCommand


version = "0.1"


class PyTest(TestCommand):

    def run_tests(self):
        import pytest
        errno = pytest.main(["tests"])
        sys.exit(errno)


setuptools.setup(
    name="dagger",
    packages=setuptools.find_packages(),
    author="TrustYou",
    author_email="development@trustyou.com",
    version=version,

    test_suite="tests",
    tests_require=[
        "pytest"
    ],
    cmdclass={"test": PyTest},
    platforms="any",
)