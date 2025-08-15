from pathlib import Path

def retrieve_local_file(path: str) -> str:
    """
    Reads a local file and returns its contents as a UTF-8 string.
    """
    return Path(path).read_text(encoding="utf-8")