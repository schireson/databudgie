# Interpolation/templating

Configuration is interpreted via [Configly](https://github.com/schireson/configly), which enables
the ability to load dynamic config values through the environment or other interpolation
methods.

For example, loading the url/secrets from the environment.

```yaml
# required
url: <% ENV[POSTGRES_URL] %>

# optional
url: <% ENV[POSTGRES_URL, 'default string'] %>
```

## Value Templating

Additionally, certain values use python's string format sub-language for allowing
the reference of context-specific state.

The following format specifiers have been implemented for referencing non-static
data in config:

| Name  | Example                                          | Description                                                                                                                            |
| ----- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| table | query: select \* from {table}                    | Templates the referenced table name into "query"'s value                                                                               |
| ref   | location: {ref.backup.tables[public.*].location} | Templates the value retrieved by following the config traversal from backup -> tables -> public.\* -> location into "location"'s value |

This feature is probably most useful when using [Globbing](table.md#globbing), as exemplified above, for
referencing the current table.
