from setuptools import find_packages, setup

install_requires = [
    'fastjsonschema',
    'xmltodict',

]

extra_requires = ['botocore', 'boto3',]

setup(
    name="cumulus_lambda_functions",
    version="1.0.0",
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=['mock', 'nose', 'sphinx', 'sphinx_rtd_theme', 'coverage'],
    test_suite='nose.collector',
    author=['Wai Phyo'],
    author_email=['wai.phyo@jpl.nasa.gov'],
    license='NONE',
    include_package_data=True,
    python_requires="==3.7",
    entry_points={
    }
)
