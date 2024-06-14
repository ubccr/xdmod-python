# Instructions for developers

## Testing the code
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
1. Install `python-dotenv` and `pytest` and `coverage`:
    ```
    python3 -m pip install --upgrade python-dotenv pytest coverage
    ```
1. Create an empty file in your home directory at `~/.xdmod-data-token` with permissions set to 600.
1. With an [ACCESS XDMoD](https://xdmod.access-ci.org) account with "User" as the Top Role, create an API token if you do not already have one (sign in and click My Profile -> API Token).
1. Add the following line to the file `~/.xdmod-data-token`, replacing `<token>` with your token.
    ```
    XDMOD_API_TOKEN=<token>
    ```
1. Change directories to your local development copy of `xdmod-data`.
1. Run the following command and make sure all the tests pass:
    ```
    coverage run -m pytest -vvs -o log_cli=true tests/
    ```
1. Run the following command and make sure code is 100% covered by tests
    ```
    coverage report -m
    ```
1. Downgrade to the minimum version of the dependencies. Replace the version numbers below with the values from `setup.cfg`.
    ```
    python3 -m pip install --force-reinstall numpy==1.23.0 pandas==1.5.0 plotly==5.8.0 requests==2.19.0
    ```
1. Run the following command again and make sure all the tests pass (Deprecation warnings in `urllib3` are Ok).
    ```
    pytest -vvs -o log_cli=true tests/
    ```
    
## Releasing a new version
1. Make a new branch of `xdmod-data` and:

    1. Make sure the version number is updated in `xdmod_data/__version__.py`.
    1. Update the Open XDMoD compatibility matrix in the `README.md`.
    1. Update `CHANGELOG.md` to change "Main development branch" to, e.g., `v1.0.1 (2024-06-XX)`.
    1. Create a Pull Request for the new version.

1. After the Pull Request is approved (but not merged yet), follow these steps in a cloned copy of the branch:
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
        Make sure you receive `PASSED`.
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
1. Update the date of the release in `CHANGELOG.md` and commit it to the Pull Request.
1. Merge the Pull Request.

1. Go to [create a new release on GitHub](https://github.com/ubccr/xdmod-data/releases/new) and:
    1. Click `Choose a tag`.
    1. Type in a tag similar to `v1.0.0` and choose `Create new tag`.
    1. Make the release title the same as the tag name.
    1. Where it says `Describe this release` paste in the contents of `CHANGELOG.md`.
    1. Where it says `Attach binaries` attach the built distribution that was uploaded to PyPI.
    1. Click `Publish release`.

## After release

1. Make a new branch of `xdmod-data` and:

    1. Make sure the version number is updated in `xdmod_data/__version__.py` to a beta release of the next version, e.g., `1.0.1-beta.1`.
    1. Update `CHANGELOG.md` to add a section at the top called `Main development branch`.
    1. Create a Pull Request for the new version.