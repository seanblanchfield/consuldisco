import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="consuldisco",
    version="0.0.1",
    author="Tilvur Ltd",
    author_email="sblanchfield@gmail.com",
    description="A service discovery utility for finding services tracked by Consul.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seanblanchfield/consuldisco",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
