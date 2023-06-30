from databudgie.api import backup, root_config


def test_backup(pg):
    config = root_config(
        raw_config="""{
            "tables": []
        }""",
    )
    backup(pg, config.backup)
