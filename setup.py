from setuptools import setup

setup(
    name="bztnewrelic",
    version="0.2.2",

    author="Oles Pisarenko",
    author_email="doctornkz@ya.ru",
    license="MIT",
    description="Python module for Taurus to stream reports to NewRelic",
    url='https://github.com/doctornkz/newrelicUploader',
    keywords=[],

    packages=["newrelic"],
    install_requires=['bzt'],
    include_package_data=True,
)