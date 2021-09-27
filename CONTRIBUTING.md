# Contributing


## Development Flow

1. Create a new branch off of latest `main` branch.
1. Make your changes.
1. Bump up the library version in the [pyproject.toml](./pyproject.toml) according to [SemVer](https://semver.org/#summary)
1. Auto-fix most linting errors via `make format`
1. Make sure linting is passing by running `make lint`.
1. Make sure tests are passing by running `make test`
1. Commit and push your changes to a remote branch.
1. Open a PR and confirm build is passing.
1. Once your PR is merged and CICD is complete, your new version will be ready to install on our Artifactory.

## Setup

### Pre-Requisites for running on your Host Machine

1. Install Pyenv and Poetry by following the instructions outlined [here](http://engineering.docs.known.is/blog/recommended-python-environment-setup/). (Must be on Known Network/VPN to access this link.)

1. Install [direnv](https://direnv.net/)

1. [Hook direnv to your shell](https://direnv.net/docs/hook.html). If you're using bash, e.g., add this to your `~/.bashrc`.

1. Create an `.envrc` file for secrets/configuration based on the [config.databudgie.yml](./config.databudgie.yml) and place it in the root of repo.

    Example:

    ```shell
    # .envrc
    export ENVIRONMENT=development
    export DATABASE_HOST=something
    export DATABASE_USERNAME=something
    export DATABASE_PASSWORD=something
    export DATABASE_PORT=something
    export DATABASE_DATABASE=something
    ```

1. `direnv allow` the `.envrc` file

1. Test it works as expected:

    ```bash
    $ databudgie backup
    ```


### Manifest Tables

The following Python will create a manifest table from the `tests.mockmodels.models.DatabudgieManifest` model.

Please supply your own SQLAlchemy connection string.

```python
from sqlalchemy import create_engine
from tests.mockmodels.models import Base

engine = create_engine('SQLALCHEMY_CONN_URL', echo=True)
Base.metadata.tables["databudgie_manifest"].create(bind=engine)
```