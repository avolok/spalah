from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

__version__ = "0.3.1"

requirements = []

test_requirements = ['pytest>=3', ]

setup(
    author="Alex Volok",
    author_email='alexandr.volok@gmail.com',
    python_requires='>=3.7',
    classifiers=[        
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',        
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    description="Spalah is a set of PySpark dataframe helpers",
    long_description=long_description,
    long_description_content_type='text/markdown',
    install_requires=requirements,
    license="MIT license",    
    include_package_data=True,
    keywords='spalah',
    name='spalah',
    packages=find_packages(include=['spalah', 'spalah.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/avolok/spalah',
    version=__version__,
    zip_safe=False,
)
