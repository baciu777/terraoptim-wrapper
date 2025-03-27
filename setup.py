from setuptools import setup, find_packages

setup(
    name="terraoptim-wrapper",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "boto3",
        "terraform",
        # Add other dependencies if needed
    ],
    entry_points={
        'console_scripts': [
            'terraoptim=main:main',
        ],
    },
)