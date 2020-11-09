import setuptools

setuptools.setup(name='med-data-project',
                 version='0.1',
                 description='mentioned drugs analysis',
                 url='https://github.com/avlessi/med-data-project',
                 author='',
                 author_email='',
                 license='MIT',
                 packages=['util'],
                 setup_requires=["pytest-runner"],
                 tests_require=["pytest"])