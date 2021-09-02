![databudgie](databudgie.png)

# databudgie

standalone library/cli tool for database backup/restore

## Installation

```bash
$ poetry add databudgie
 OR
$ pip install databudgie --index-url "https://artifactory.schireson.com/artifactory/api/pypi/pypi/simple"
```

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

1. Create an `.envrc` file for secrets/configuration based on the [config.yml](./config.yml) and place it in the root of repo.

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

    ```python
    $ python
    ...
    Python 3.7.5 (default, Nov 12 2019, 16:26:22)
    [Clang 10.0.1 (clang-1001.0.46.4)] on darwin
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import databudgie
    >>> config = databudgie.get_config()
    >>> if "development" == config.environment:
    >>>     print("YAHOO")
    YAHOO
    ```

### Pre-Requisites for running in Docker

1. Install [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html)

1. Setup your AWS Credentials

    Follow the detailed instructions on the [Beeswax Manager README](https://github.com/schireson/media-activation-beeswax-manager#setup-aws-credentials-locally)

1. Pull the latest docker image from ECR or build one locally

    ```shell
    $ make docker-login-cmd
    $ make pull

    // OR

    $ make build
    ```

1. Create a `.env` file for secrets/configuration based on the [config.yml](./config.yml) and place it in the root of repo.

    Each line should be in the `VAR=VAL` format as described [here](https://docs.docker.com/compose/env-file/).

    Example:

    ```shell
    # .env
    ENVIRONMENT=development
    DATABASE_HOST=something
    DATABASE_USERNAME=something
    DATABASE_PASSWORD=something
    DATABASE_PORT=something
    DATABASE_DATABASE=something
    ```

1. Enter a Container

    ```shell
    $ make enter
    ```

1. Test it works as expected:

    ```python
    container$ python
    ...
    Python 3.7.5 (default, Nov 12 2019, 16:26:22)
    [Clang 10.0.1 (clang-1001.0.46.4)] on darwin
    Type "help", "copyright", "credits" or "license" for more information.
    >>> import databudgie
    >>> config = databudgie.main()
    >>> if "development" == config.environment:
    >>>     print("YAHOO")
    YAHOO
    ```

## Database Connection Info

#### Connecting from Command Line (mac and linux)

Append the following to your ssh config file at `~/.ssh/config`. This is the configuration will make it easy to create the ssh tunnel.

```
Host media-activation-prod-pg
    Hostname 35.168.186.219
    User ec2-user
    LocalForward 15432 prod-data-infra-media-activation.c7brdmpdwjli.us-east-1.rds.amazonaws.com:5432
    IdentityFile ~/.ssh/id_rsa
```

Now, to open the tunnel run the following:

```
ssh media-activation-prod-pg
```

## Useful Tips

* Add a python dependency and install it in your current env

    ```shell
    $ poetry add dependency-name[=OPTIONAL-VERSION]
    ```

* Add a dev python dependency and install it in your current env

    ```shell
    $ poetry add dependency-name[=OPTIONAL-VERSION] --dev
    ```

* Update platform-actions versions

    ```shell
    $ make update-platform-actions

    // Commit the change for you bretheren!
    $ git add pyproject.toml poetry.lock
    $ git commit -m "Update platform-actions version."
    $ git push...
    ```

* Creating a python "bin" script

    1. Create a python script in the bin directory with a dash-cased name. i.e. `bin/log-example.py`

    1. Paste the following template into your file:

        ```python
        #!/usr/bin/env python3

        """<Sentence to indicate general purpose of script>.

        USAGE:
            $ bin/example.py
            <EXAMPLE OUTPUT>
            ....
        """

        import logging

        from configly import Config

        from platform_actions.utilities.logging import setup_logging
        from databudgie import get_config

        config = get_config()
        setup_logging(config.logging, verbosity=1)

        log = logging.getLogger("__name__")


        def some_fn_with_top_level_script_logic():
            x = _other_fn_with_script_logic_in_smaller_chunks()
            y = _other_fn_with_script_logic_in_smaller_chunks_2()


        def _other_fn_with_script_logic_in_smaller_chunks():
            pass


        def _other_fn_with_script_logic_in_smaller_chunks_2():
            pass


        if __name__ == "__main__":
            # At-runtime constants are created here, i.e. clients and date values

            # Finally call your top-level script fn
            some_fn_with_top_level_script_logic()
        ```

    1. Change permissions on your file so that it is executable by `user`:

        ```shell
        $ chmod u+x bin/<YOUR-SCRIPT>.py
        ```

    1. Git Add, Commit, Push and PR!
