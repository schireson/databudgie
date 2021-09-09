import re

from configly import Config

from databudgie.utils import csv_path


def substitute_locations(targets: Config, sources: Config) -> Config:
    """Replace references to other table locations with the actual S3 path.
    """
    output = Config(targets)

    regex = re.compile(r"\$\{location:(.*)\}")
    for table_name, conf in targets.items():
        if match := regex.match(conf["location"]):
            source_name = match.group(1)
            source_location = sources[source_name]["location"]
            output[table_name]._value["location"] = csv_path(source_location, source_name)

    return output
