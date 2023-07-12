# Developing a new version
1. Make sure the version number is updated in `xdmod_data/__version__.py`.
1. Create a Pull Request for the new version.

# Distributing the new version to PyPI
After the Pull Request is approved, follow these steps in the `xdmod-data` root directory:
1. Start up a virtual environment, e.g.:
    ```
    $ env_dir=~/xdmod-data-build-env
    $ python3 -m venv ${env_dir}
    $ source ${env_dir}/bin/activate
    ```
1. Make sure the required packages are installed:
    ```
    (env) $ python3 -m pip install --upgrade pip build setuptools twine
    ```
1. Build the built distribution:
    ```
    (env) $ python3 -m build --wheel
    ```
1. Upload the built distribution to PyPI, e.g., for version 1.0.0-beta1:
    ```
    (env) $ version=1.0.0b1
    (env) $ twine upload dist/xdmod_data-${version}-py3-none-any.whl
    ```
    Enter your PyPI username and password.

# Updating the API documentation
Follow these steps in the `docs` directory:
1. Start up a virtual environment, e.g.:
    ```
    $ env_dir=~/xdmod-data-build-env
    $ python3 -m venv ${env_dir}
    $ source ${env_dir}/bin/activate
    ```
1. Make sure the required packages are installed:
    ```
    (env) $ python3 -m pip install --upgrade pip xdmod-data sphinx sphinx-rtd-theme
    ```
1. Make the HTML documentation:
    ```
    (env) $ make html
    ```
1. In a checked out copy of the `gh-pages` branch of the `xdmod` GitHub repository, replace the contents of the `data-analytics-framework/api/` directory with the contents of the `_build/html` directory, excluding `.buildinfo` and `_sources`.
1. Commit your changes to the `gh-pages` branch and submit a Pull Request to the `gh-pages` branch of `ubccr/xdmod`.
