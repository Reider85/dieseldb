"""Packaging for the DieselDB Python driver (PyPI: diesel-python)."""

from setuptools import setup, find_packages

setup(
    name="diesel-python",
    version="0.1.0",
    description="Pure-Python psycopg2-compatible driver for DieselDB "
                "(libpq wire protocol)",
    long_description=(
        "Pure-Python, zero-dependency driver for DieselDB exposing a "
        "psycopg2-compatible API: connect(), cursor(), execute() with %s / "
        "%(name)s pyformat parameters, fetchone/fetchall, plus an asyncio "
        "variant (diesel.aio)."
    ),
    long_description_content_type="text/plain",
    author="DieselDB Project",
    url="https://github.com/Reider85/dieseldb",
    license="MIT",
    packages=find_packages(exclude=("tests", "tests.*")),
    python_requires=">=3.8",
    install_requires=[],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Database :: Front-Ends",
    ],
)
