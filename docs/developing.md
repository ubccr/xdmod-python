# Instructions for developers

## Testing the code

A testing script is available in `tests/ci/bootstrap.sh`. It requires Docker
Compose and `yq`.

To test with the notebooks in `xdmod-notebooks`, you can edit their first code
cell to replace `xdmod-data` and its version constraints with the following,
replacing `username` with your username and `branch-name` with the name of the
branch:
```
git+https://github.com/username/xdmod-data.git@branch-name
```

## Releasing a new version

1. Make a new branch of `xdmod-data` and:
    1. Make sure the version number is updated in `xdmod_data/__version__.py`.
    1. In `README.md`:
        1. Under the main heading,
            1. In the sentence that begins, `This documentation is for ...`,
               replace the version number in bold, e.g.:
                ```
                This documentation is for **v1.0.2**.
                ```
        1. Update the Open XDMoD compatibility matrix.
    1. Update `CHANGELOG.md` to:
        1. Change the `development branch` to, e.g., `v1.0.1 (2024-06-XX)`.
        1. Add a summary of the changes in the version, including the
           compatibilty with Open XDMoD versions.
    1. In `setup.cfg`, update the `long_description` to change the version
       number in the URL to the new version.
    1. Create a Pull Request for the new version.
1. After the Pull Request is approved (but not merged yet), follow these steps
   in a cloned copy of the branch:
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
    1. Go to https://test.pypi.org/project/xdmod-data and confirm that
       everything looks right.
    1. Go to https://test.pypi.org/manage/project/xdmod-data/releases/ and:
        1. In the `Options` menu next to the release, click `Delete`.
        1. Type the version number in the box.
        1. Click `Delete release`.
    1. Upload the built distribution to PyPI:
        ```
        twine upload dist/xdmod_data-${version}-py3-none-any.whl
        ```
        Enter your PyPI username and password.
    1. Go to https://pypi.org/project/xdmod-data and confirm the new version is
       the latest release.
1. Update the date of the release in `CHANGELOG.md` and commit it to the Pull
   Request.
1. Merge the Pull Request.
1. Go to [create a new release on GitHub](https://github.com/ubccr/xdmod-data/releases/new)
   and:
    1. Click `Choose a tag`.
    1. Type in a tag similar to `v1.0.0` and choose `Create new tag`.
    1. Choose the correct target based on the major version you are developing.
    1. Make the release title the same as the tag name (e.g., `v1.0.0`).
    1. Where it says `Describe this release`, paste in the contents of the
       release's section in `CHANGELOG.md`. Note that single newlines are
       interpreted as line breaks, so you may need to reformat the description
       to break the lines where you want them to break.
    1. Where it says `Describe this release`, paste in the contents of the
       release's section in `CHANGELOG.md`.
    1. Where it says `Attach binaries`, attach the built distribution (the
       `.whl` file) that was uploaded to PyPI.
    1. Click `Publish release`.
    1. Go to the [GitHub milestones](https://github.com/ubccr/xdmod-data/milestones)
       and close the milestone for the version.

## After release

1. If this is a minor or patch release to a version that is not the most recent
   major version,
    1. For each major version above this release's major version, in a Pull
       Request,
        1. Add the entry for this version to the `CHANGELOG.md`.
        1. In the `README.md`:
            1. Add an item to the top of the bulleted list for
               the new version, making sure to replace the version number in
               the link text and in the URL.
            1. Update the Open XDMoD compatibility matrix.
1. In a Pull Request to the `main` branch of `xdmod-data`:
    1. Make sure the version number is updated in `xdmod_data/__version__.py`
       to a development pre-release of the next version, e.g., `1.0.1.dev01`.
    1. In `README.md`, under the main heading, in the sentence that begins,
       `This documentation is for ...`, replace the version number in bold,
       e.g.:
        ```
        This documentation is for **v1.x.y (development branch)**.
        ```
    1. Update `CHANGELOG.md` to add a section at the top called
       `v1.x.y (development branch)`, replacing `1` with the major version
       under development (add `main` before `development branch` if it is also
       the main development branch.
1. If this is a new major version,
    1. Create a branch off `main` for the previous major version called, e.g.,
       `v1.x.y`.
1. Go to the [GitHub milestones](https://github.com/ubccr/xdmod-data/milestones)
   and add a milestone for the version under development.
1. Make a new branch of `xdmod-data` and:
    1. Make sure the version number is updated in `xdmod_data/__version__.py`
       to a pre-release of the next version, e.g., `1.0.1.dev01`.
    1. Update `CHANGELOG.md` to add a section at the top called `Main
       development branch`.
    1. Go to the [GitHub milestones](https://github.com/ubccr/xdmod-data/milestones)
       and add a milestone for the version.
    1. Create a Pull Request for the new version.
    1. Once the Pull Request is approved, merge it into `main`.
