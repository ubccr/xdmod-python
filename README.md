# xdmod-data
As part of the Data Analytics Framework for [XDMoD](https://open.xdmod.org), this Python package provides API access to the data warehouse of instances of Open XDMoD.

The package can be installed from PyPI via `pip install xdmod-data`.

It has dependencies on [NumPy](https://pypi.org/project/numpy/), [Pandas](https://pypi.org/project/pandas/), [Plotly](https://pypi.org/project/plotly/), and [Requests](https://pypi.org/project/requests/).

Example usage is documented through Jupyter notebooks in the [xdmod-notebooks](https://github.com/ubccr/xdmod-notebooks) repository.

## Compatibility with Open XDMoD
Specific versions of this `xdmod-data` API are compatible with specific versions of Open XDMoD as indicated in the table below.

| `xdmod-data` version | Open XDMoD versions |
| -------------------- | ------------------- |
| 1.0.1                | 11.0.x, 10.5.x      |
| 1.0.0                | 10.5.x              |

## API Token Access
Use of the Data Analytics Framework requires an API token. To obtain an API token, follow the steps below to obtain an API token from the XDMoD portal.

1. First, if you are not already signed in to the portal, sign in in the top-left corner:

    ![Screenshot of "Sign In" button](https://raw.githubusercontent.com/ubccr/xdmod-data/main/docs/images/api-token/sign-in.jpg)

1. Next, click the "My Profile" button in the top-right corner:

    ![Screenshot of "My Profile" button](https://raw.githubusercontent.com/ubccr/xdmod-data/main/docs/images/api-token/my-profile.jpg)

1. The "My Profile" window will appear. Click the "API Token" tab:

    ![Screenshot of "API Token" tab](https://raw.githubusercontent.com/ubccr/xdmod-data/main/docs/images/api-token/api-token-tab.jpg)

    **Note:** If the "API Token" tab does not appear, it means this instance of XDMoD is not configured for the Data Analytics Framework.

1. If you already have an existing token, delete it:

    ![Screenshot of "Delete API Token" button](https://raw.githubusercontent.com/ubccr/xdmod-data/main/docs/images/api-token/delete.jpg)

1. Click the "Generate API Token" button:

    ![Screenshot of "Generate API Token" button](https://raw.githubusercontent.com/ubccr/xdmod-data/main/docs/images/api-token/generate.jpg)

1. Copy the token to your clipboard. Make sure to paste it somewhere secure for saving, as you will not be able to see the token again once you close the window:

    ![Screenshot of "Copy API Token to Clipboard" button](https://raw.githubusercontent.com/ubccr/xdmod-data/main/docs/images/api-token/copy.jpg)

    **Note:** If you lose your token, simply delete it and generate a new one.

## Feedback / Feature Requests
We welcome your feedback and feature requests for the XDMoD Data Analytics Framework via email: ccr-xdmod-help@buffalo.edu.

## Support
For support, please see [this page](https://open.xdmod.org/support.html). If you email for support, please include the following:
* `xdmod-data` version number, obtained by running this Python code:
    ```
    from xdmod_data import __version__
    print(__version__)
    ```
* Operating system version.
* A description of the problem you are experiencing.
* Detailed steps to reproduce the problem.

## License
`xdmod-data` is released under the GNU Lesser General Public License ("LGPL") Version 3.0. See the [LICENSE](LICENSE) file for details.

## References

When referencing the Data Analytics Framework for XDMoD, please cite the following publication:

> Weeden, A., White, J.P., DeLeon, R.L., Rathsam, R., Simakov, N.A., Saeli, C., and Furlani, T.R. The Data Analytics Framework for XDMoD. _SN COMPUT. SCI._ 5, 462 (2024). https://doi.org/10.1007/s42979-024-02789-2

When referencing XDMoD, please cite the following publication:

> Jeffrey T. Palmer, Steven M. Gallo, Thomas R. Furlani, Matthew D. Jones, Robert L. DeLeon, Joseph P. White, Nikolay Simakov, Abani K. Patra, Jeanette Sperhac, Thomas Yearke, Ryan Rathsam, Martins Innus, Cynthia D. Cornelius, James C. Browne, William L. Barth, Richard T. Evans, "Open XDMoD: A Tool for the Comprehensive Management of High-Performance Computing Resources", *Computing in Science & Engineering*, Vol 17, Issue 4, 2015, pp. 52-62. DOI:[10.1109/MCSE.2015.68](https://doi.org/10.1109/MCSE.2015.68)

