# Testing the code
1. Start up a virtual environment, e.g.:
    ```                                            
    python3 -m venv ~/xdmod-data-test-env                                             
    source ~/xdmod-data-test-env/bin/activate
    ```
    Your command prompt should now start with `(xdmod-data-test-env)`.
1. Install your local development copy of `xdmod-data` in editable mode:
    ```
    python3 -m pip install --force-reinstall -e /path/to/your/xdmod-data
    ```
1. Install `python-dotenv` and `pytest`:
    ```
    python3 -m pip install --upgrade python-dotenv pytest
    ```
1. Create an empty file in your home directory at `~/.xdmod-data-token` with permissions set to 600.
1. With an [https://xdmod.access-ci.org](ACCESS XDMoD) account with "User" as the Top Role, create an API token if you do not already have one (sign in and click My Profile -> API Token).
1. Add the following line to the file `~/.xdmod-data-token`, replacing `<token>` with your token.
    ```
    XDMOD_API_TOKEN=<token>
    ```
1. Change directories to your local development copy of `xdmod-data`.
1. Run the following command and make sure all the tests pass:
    ```
    pytest -vvs -o log_cli=true tests/ 
    ```
1. Downgrade to the minimum version of the dependencies. Replace the version numbers below with the values from `setup.cfg`.
    ```
    python3 -m pip install --force-reinstall numpy==1.23.0 pandas==1.5.0 plotly==5.8.0 requests==2.19.0
    ```
1. Run the following command again and make sure all the tests pass (Deprecation warnings in `urllib3` are Ok).
    ```
    pytest -vvs -o log_cli=true tests/
    ```
    
# Preparing a new version for release
1. Make sure the version number is updated in `xdmod_data/__version__.py`.
1. Update the Open XDMoD compatibility matrix in the `README.md`
1. Update CHANGELOG.md to change "Main development branch" to v1.0.1 (2024-06-XX)
1. Create a Pull Request for the new version.
1.Do not merge the Pull Request until following the instructions below for distributing the new version.

# Distributing the new version to PyPI
After the Pull Request is approved (but not merged), follow these steps in the `xdmod-data` root directory:
1. Start up a virtual environment, e.g.:
    ```
    python3 -m venv ~/xdmod-data-build-env
    source ~/xdmod-data-build-env/bin/activate
    ```
    Your command prompt should now start with `(xdmod-data-build-env)`.
1. Make sure the required packages are installed:
    ```
    python3 -m pip install --upgrade pip build setuptools twine
    ```
1. Build the built distribution:
    ```
    python3 -m build --wheel
    ```
1. Validate `setup.cfg`, e.g., for version 1.0.0-beta1:
    ```
    version=1.0.0b1
    twine check dist/xdmod_data-${version}-py3-none-any.whl
    ```
    Make sure you recieve `PASSED`.
1. Upload the built distribution to TestPyPI:
    ```
    twine upload --repository testpypi dist/xdmod_data-${version}-py3-none-any.whl
    ```
    Enter your TestPyPI API token.
1. Go to https://testpypi.org/project/xdmod-data and confirm that everything looks right.
1. Upload the built distribution to PyPI:
    ```
    twine upload dist/xdmod_data-${version}-py3-none-any.whl
    ```
    Enter your PyPI username and password.
1. Go to https://pypi.org/project/xdmod-data and confirm the new version is the latest release.
1. In the Pull Request, update the date of the release in `CHANGELOG.md`.
1. Merge the Pull Request.
