import setuptools

setuptools.setup(
    packages=setuptools.find_packages(),
    include_package_data=True,
    scripts=[
        "bin/kachery-client",
        "bin/kachery-cat",
        "bin/kachery-load",
        "bin/kachery-store",
        "bin/kachery-link",
        "bin/kachery-info",
        "bin/kachery-generate-node-id"
    ],
    install_requires=[
        "click",
        "simplejson",
        "requests",
        "jinjaroot"
    ]
)
