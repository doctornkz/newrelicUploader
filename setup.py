from setuptools import setup

setup(
    name="bztnewrelic",
    version="0.3.0",

    author="Oles Pisarenko",
    author_email="doctornkz@ya.ru",
    license="MIT",
    description="Python module for Taurus to stream reports to NewRelic",
    url='https://github.com/doctornkz/newrelicUploader',
    keywords=[],

    packages=["bztnewrelic"],
    install_requires=[
        'bzt',
        'requests',
        'newrelic-telemetry-sdk',
        'python_graphql_client'],
    include_package_data=True,
)