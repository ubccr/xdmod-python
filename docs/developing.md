# Testing the code
1. Start up a virtual environment, e.g.:
    ```
    $ env_dir=~/xdmod-data-test-env                                            
    $ python3 -m venv ${env_dir}                                                
    $ source ${env_dir}/bin/activate
    ```
1. Install your local development copy of `xdmod-data` in editable mode:
    ```
    (env) $ python3 -m pip install --force-reinstall -e /path/to/your/xdmod-data
    ```
1. Install `python-dotenv` and `pytest`:
    ```
    (env) $ python3 -m pip install --upgrade python-dotenv pytest
    ```
1. Create an empty file in your home directory at `~/.xdmod-data-token` with permissions set to 600.
1. With an [https://xdmod.access-ci.org](ACCESS XDMoD) account with "User" as the Top Role, create an API token if you do not already have one (sign in and click My Profile -> API Token).
1. Add the following line to the file `~/.xdmod-data-token`, replacing `<token>` with your token.
    ```
    XDMOD_API_TOKEN=<token>
    ```
1. Change directories to your local development copy of `xdmod-data`.
1. Run the following command:
    ```
    (env) $ pytest -vvs -o log_cli=true tests/ 
    ```

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
