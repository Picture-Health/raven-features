from setuptools import setup, find_packages

setup(
    name="raven_features",
    version="0.0.2",
    packages=find_packages(),
    install_requires=[
        "click",
        "clearml",
        "boto3",
        "email-validator",
        "pydantic",
        "raven @ git+https://github.com/Picture-Health/raven.git"
    ],
    entry_points={
        # Defines CLI commands that map to specific functions within the project.
        "console_scripts": [
            "featurize=raven_features.cli:featurize",
        ]
    },
    # Package metadata to help users find and understand the package.
    authors=["David Levy", "Rushil Nagabhushan", "Diego Cantor"],
    description=(
        "RAVEN FEATURES is a Python package for orchestrating end-to-end featurization pipelines "
        "on medical imaging data. It enables dynamic job launching via ClearML, powered by configurable YAML specs, "
        "and supports distributed processing of imaging series across modular steps such as data provisioning, "
        "lesion splitting, radiomic extraction, and feature group upserts. "
        "Designed for flexibility and scalability, RAVEN FEATURES integrates tightly with the RAVEN ecosystem, "
        "ensuring that high-quality, standardized features are efficiently extracted and prepared for modeling workflows."
    ),
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Picture-Health/raven-features",
    python_requires=">=3.10",
)