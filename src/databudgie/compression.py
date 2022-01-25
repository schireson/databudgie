import gzip
import io
from typing import Dict, Optional, Type


class Compressor:
    name: str = ""
    extension: str = ""

    compressors: Dict[Optional[str], "Compressor"] = {}

    @classmethod
    def get_with_name(cls, name: Optional[str]) -> "Compressor":
        return cls.compressors.get(name, Compressor())

    @classmethod
    def __init_subclass__(cls: Type["Compressor"]):
        Compressor.compressors[cls.name] = cls()

    def compose_filetype(self, filetype: str):
        if not self.extension:
            return filetype

        return ".".join([filetype, self.extension])

    @staticmethod
    def compress(buffer):
        return buffer

    @staticmethod
    def extract(buffer):
        return buffer


class GzipCompressor(Compressor):
    name: str = "gzip"
    extension: str = "gz"

    @staticmethod
    def compress(result):
        buffer = io.BytesIO()
        with gzip.open(buffer, mode="wb") as f:
            f.write(result.getvalue())
        buffer.seek(0)
        return buffer

    @staticmethod
    def extract(buffer):
        return gzip.open(buffer, mode="rb")
