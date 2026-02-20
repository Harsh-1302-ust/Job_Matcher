from pathlib import Path
import json


def _load():
    root = Path(__file__).resolve().parents[2]
    p = root / "technologies_categories.json"
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []


TECH_CATEGORIES_LIST = _load()
TECH_CATEGORIES_JSON_STR = json.dumps(TECH_CATEGORIES_LIST, indent=2)
TECH_CATEGORIES_MAP = {
    item.get("Technology", ""): item.get("Categories", [])
    for item in TECH_CATEGORIES_LIST
    if isinstance(item, dict)
}
