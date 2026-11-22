"""Download NYC Open Data (Socrata) datasets into the bronze landing volume."""

import json, time, requests
from pathlib import Path
from typing import Any
from src.config import SOCRATA_BASE as BASE, LANDING, PAGE_SIZE

Row = dict[str, Any]

def _get(url: str, params: dict[str, Any], tries: int = 5) -> list[Row]:
    for i in range(tries):
        try:
            r = requests.get(url, params=params, timeout=120)
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if i == tries - 1: 
                raise
            time.sleep(2 ** i)
    raise RuntimeError("unreachable")

def download(dataset_id: str, name: str, page_size: int = PAGE_SIZE, max_pages: int = 10_000) -> int:
    """Save a dataset to the landing volume, one JSON-lines file per page. Returns rows saved."""
    out = Path(LANDING, name, time.strftime("%Y-%m-%d_%H%M%S"))
    out.mkdir(parents=True, exist_ok=True)
    url = BASE.format(dataset_id)

    total = 0
    for page in range(max_pages):
        params = {"$select": ":*, *", "$order": ":id", "$limit": page_size, "$offset": total}
        rows = _get(url, params)
        if not rows:
            break
        lines = [json.dumps(row) for row in rows]
        (out / f"page_{page:04d}.json").write_text("\n".join(lines))
        total += len(rows)

    print(f"{name}: {total:,} rows saved to {out}")
    return total