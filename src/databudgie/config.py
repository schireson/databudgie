from configly import Config


def populate_location_ref(config: Config) -> Config:
    """Replace property references with the referenced values.

    Traverse through the config object and replace values like `${ref:backup.table.mytable.location}`
    with the value found at the referenced location.
    """

    return config

    # regex = re.compile(r"\$\{ref:(.*)\}") # matches `${ref:somevalue}`

    # if match := regex.match(conf["location"]):
    #     source_name = match.group(1)
    #     source_location = sources[source_name]["location"]
    #     output[table_name]._value["location"] = ???

    # return output

    # def recurse(conf): # this doens't work yet
    #     for key, value in conf:
    #         if isinstance(value, str): # if value is string, check and perform replacement
    #             if match := regex.match(value):
    #                 source_name = match.group(1)
    #                 source_location = sources[source_name]["location"] # wrong
    #                 conf._value[key] = source_location # also wrong
    #             return conf
    #         elif isinstance(value, Config): # if value is config, recurse
    #             return recurse(value)
    #     return conf
