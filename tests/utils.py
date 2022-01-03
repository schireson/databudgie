from configly import Config


def make_config(*, backup=None, restore=None, ddl=False, clean=False):
    raw_config = {}

    if backup:
        raw_config["backup"] = backup

    if restore:
        raw_config["restore"] = {
            "ddl": {
                "enabled": ddl,
                "clean": clean,
            },
            **restore,
        }

    return Config(raw_config)
