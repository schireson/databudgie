from databudgie.config import ConfigStack, RootConfig


def test_only_leaf_values():
    """These config values should ignore all inherited values and use the leaf-specified values"""

    config = RootConfig.from_dict(
        {
            "url": "root_url",
            "query": "root_query",
            "location": "root_location",
            "backup": {
                "url": "backup_url",
                "query": "backup_query",
                "location": "backup_location",
                "tables": [
                    {
                        "name": "backup_table_1",
                        "query": "backup_table_1_query",
                        "location": "backup_table_1_location",
                    },
                    {
                        "name": "backup_table_2",
                        "query": "backup_table_2_query",
                        "location": "backup_table_2_location",
                    },
                ],
            },
        }
    )
    assert config.backup.tables[0].name == "backup_table_1"
    assert config.backup.tables[0].query == "backup_table_1_query"
    assert config.backup.tables[0].location == "backup_table_1_location"
    assert config.backup.tables[1].name == "backup_table_2"
    assert config.backup.tables[1].query == "backup_table_2_query"
    assert config.backup.tables[1].location == "backup_table_2_location"


def test_mixing_inherited_and_leaf_values():
    config = RootConfig.from_dict(
        {
            "url": "root_url",
            "query": "root_query",
            "location": "root_location",
            "strategy": "root_strategy",
            "backup": {
                "url": "backup_url",
                "query": "backup_query",
                "location": "backup_location",
                "tables": [
                    {
                        "name": "backup_table_1",
                        "query": "backup_table_1_query",
                        "location": "backup_table_1_location",
                    },
                    {
                        "name": "backup_table_2",
                        # these missing values should be populated by the `backup` level values
                        # "query": "backup_table_2_query",
                        # "location": "backup_table_2_location",
                    },
                ],
            },
            "restore": {
                "tables": [
                    {
                        "name": "restore_table_1",
                        "strategy": "restore_table_1_strategy",
                        "location": "restore_table_1_location",
                    },
                    {
                        "name": "restore_table_2",
                        # these missing values should be populated by the root level values
                        # "query": "restore_table_2_query",
                        # "location": "restore_table_2_location",
                    },
                ]
            },
        }
    )

    assert config.backup.tables[0].name == "backup_table_1"
    assert config.backup.tables[0].query == "backup_table_1_query"
    assert config.backup.tables[0].location == "backup_table_1_location"
    assert config.backup.tables[1].name == "backup_table_2"
    assert config.backup.tables[1].query == "backup_query"
    assert config.backup.tables[1].location == "backup_location"

    assert config.restore.tables[0].name == "restore_table_1"
    assert config.restore.tables[0].strategy == "restore_table_1_strategy"
    assert config.restore.tables[0].location == "restore_table_1_location"
    assert config.restore.tables[1].name == "restore_table_2"
    assert config.restore.tables[1].strategy == "root_strategy"
    assert config.restore.tables[1].location == "root_location"


def test_root_level_tables():
    config = RootConfig.from_dict(
        {
            "url": "root_url",
            "query": "root_query",
            "location": "root_location",
            "strategy": "root_strategy",
            "truncate": True,
            "tables": [
                {
                    "name": "root_table_1",
                },
                {
                    "name": "root_table_2",
                },
            ],
            "backup": {
                "url": "backup_url",
            },
            "restore": {
                "url": "restore_url",
            },
        }
    )

    assert config.backup.url == "backup_url"
    assert config.backup.tables[0].name == "root_table_1"
    assert config.backup.tables[0].query == "root_query"
    assert config.backup.tables[0].location == "root_location"
    assert config.backup.tables[1].name == "root_table_2"
    assert config.backup.tables[1].query == "root_query"
    assert config.backup.tables[1].location == "root_location"

    assert config.restore.url == "restore_url"
    assert config.restore.tables[0].name == "root_table_1"
    assert config.restore.tables[0].strategy == "root_strategy"
    assert config.restore.tables[0].location == "root_location"
    assert config.restore.tables[0].truncate is True
    assert config.restore.tables[1].name == "root_table_2"
    assert config.restore.tables[1].strategy == "root_strategy"
    assert config.restore.tables[1].location == "root_location"
    assert config.restore.tables[1].truncate is True


def test_tables_as_just_strings():
    config = RootConfig.from_dict(
        {
            "url": "root_url",
            "query": "root_query",
            "location": "root_location",
            "tables": [
                "root_table_1",
                "root_table_2",
            ],
        }
    )

    assert config.backup.url == "root_url"
    assert config.backup.tables[0].name == "root_table_1"
    assert config.backup.tables[0].query == "root_query"
    assert config.backup.tables[0].location == "root_location"
    assert config.backup.tables[1].name == "root_table_2"
    assert config.backup.tables[1].query == "root_query"
    assert config.backup.tables[1].location == "root_location"


def test_tables_mixed_str_dict():
    config = RootConfig.from_dict(
        {
            "url": "root_url",
            "query": "root_query",
            "location": "root_location",
            "tables": [
                "table_1",
                {"name": "table_2", "query": "table_2_query"},
                {"name": "table_3", "location": "table_3_location"},
            ],
        }
    )

    assert config.backup.url == "root_url"
    assert config.backup.tables[0].name == "table_1"
    assert config.backup.tables[0].query == "root_query"
    assert config.backup.tables[0].location == "root_location"
    assert config.backup.tables[1].name == "table_2"
    assert config.backup.tables[1].query == "table_2_query"
    assert config.backup.tables[1].location == "root_location"
    assert config.backup.tables[2].name == "table_3"
    assert config.backup.tables[2].query == "root_query"
    assert config.backup.tables[2].location == "table_3_location"


def test_no_tables():
    config = RootConfig.from_dict(
        {
            "url": "root_url",
            "query": "root_query",
            "location": "root_location",
            "strategy": "root_strategy",
            "truncate": True,
            "backup": {
                "url": "backup_url",
            },
            "restore": {
                "url": "restore_url",
            },
        }
    )
    assert config.backup.tables == []
    assert config.restore.tables == []


def test_configs_stack():
    """Assert a stack of configs gracefully fall back to settings further up the stack."""

    config_stack = ConfigStack(
        {"url": "root_url"},
        {"query": "foo", "restore": {"url": "restore url"}},
        {"tables": [{"name": "1", "query": "bar"}, {"name": "2"}]},
    )
    config = RootConfig.from_stack(config_stack)

    assert config.backup.url == "root_url"
    assert config.restore.url == "restore url"

    assert config.backup.tables[0].name == "1"
    assert config.backup.tables[0].query == "bar"

    assert config.backup.tables[1].name == "2"
    assert config.backup.tables[1].query == "foo"
