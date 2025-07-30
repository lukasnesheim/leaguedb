# package imports
import json

# function imports
from pathlib import Path

ENUM_PATH = Path(__file__).parent.parent / "enum.json"

def get_enum(enum: str) -> dict[str, str]:
    enums = get_enums()
    
    if enum not in enums:
        raise ValueError(f"{enum=} does not exist in enum.json.")
    
    return enums.get(enum, {})

def get_enums() -> dict[str, dict[str, str]]:
    with open(ENUM_PATH, "r", encoding="utf-8") as file:
        return json.load(file)