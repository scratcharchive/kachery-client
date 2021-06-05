import setuptools

setuptools.setup(
    packages=setuptools.find_packages(),
    include_package_data=True,
    scripts=[
        "bin/kachery-client",
        "bin/kachery-cat",
        "bin/kachery-load",
        "bin/kachery-store",
        "bin/kachery-link"
    ],
    install_requires=[
        "click",
        "simplejson",
        "requests",
        "jinjaroot"
    ]
)
