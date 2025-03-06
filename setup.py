#!/usr/bin/env python3

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip()]

setup(
    name="ytmusicfs",
    version="0.1.0",
    author="astrovm",
    author_email="ytmusicfs@4st.li",
    description="YouTube Music FUSE filesystem",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/astrovm/ytmusicfs",
    packages=find_packages(),
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "ytmusicfs=ytmusicfs.cli:main",
            "ytmusicfs-oauth=ytmusicfs.utils.oauth_setup:main",
        ],
    },
)
