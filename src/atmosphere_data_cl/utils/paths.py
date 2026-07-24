"""Path handling: resolving existing paths, and building new ones from templates.

``manage_path`` is lifted from ClimateGraph's ``utils/general_utils.py``. The
templating half is new — ClimateGraph only ever resolved paths that already
existed (``save_to`` was a literal path from YAML), whereas a Store has to
*generate* output paths from the data it is writing.
"""

import glob
import logging
import os
import re
import unicodedata
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def manage_path(paths: str | Path | list[str] | list[Path], sort: bool = False) -> list[Path]:
    """Resolve paths, including lists and glob patterns, to existing files.

    When ``sort`` is True and the sorted order differs from the input order, a
    warning is logged: callers that concat in input order will otherwise get a
    silently non-monotonic time axis if filenames don't embed a sortable
    timestamp.
    """
    if isinstance(paths, (str, Path)):
        paths = [paths]

    result: list[Path] = []

    for raw in paths:
        p = Path(raw) if isinstance(raw, str) else raw
        p = p.resolve() if not p.is_absolute() else p

        matches = glob.glob(str(p))
        if not matches:
            log.debug(f"No files match pattern: {raw}")
        result.extend(Path(m).resolve() for m in matches if Path(m).exists())

    if not result:
        log.debug(f"No files exist for paths: {paths}")

    if sort:
        ordered = sorted(result)
        if ordered != result:
            log.warning(
                "manage_path: input paths were not in lexicographic order; "
                "sorted automatically. Confirm filenames embed a sortable "
                "timestamp or callers that concat in input order will produce "
                "a non-monotonic time axis. First few: %s",
                [p.name for p in ordered[:3]],
            )
        result = ordered

    return result


def safe_name(label: str) -> str:
    """Accent-strip, lowercase and underscore a label so it is filesystem-safe.

    Lifted from vipnet-scrapper, where it turns "Precipitación" into
    "precipitacion" for both column and file names.
    """
    decomposed = unicodedata.normalize("NFKD", str(label))
    stripped = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^\w\s-]", "", stripped).strip().lower()
    return re.sub(r"[\s-]+", "_", cleaned)


_FIELD_RE = re.compile(r"\{(\w+)\}")


def template_fields(template: str) -> list[str]:
    """Field names referenced by a path template, in order of appearance."""
    return _FIELD_RE.findall(template)


def template_to_glob(template: str) -> str:
    """Turn a path template into a glob that matches any rendering of it.

    ``"{variable}/{time}.{ext}"`` → ``"*/*.*"``. Used to enumerate every raw
    file laid out under a template without knowing the field values.
    """
    return _FIELD_RE.sub("*", template)


def render_template(template: str, values: dict[str, Any]) -> Path:
    """Render a path template like ``{year}/{month}/{day}.parquet``.

    Integer-valued time fields are zero-padded to their conventional width so
    lexicographic ordering matches chronological ordering — without this,
    ``2026/7/`` sorts after ``2026/12/`` and every downstream glob-and-concat
    silently produces a non-monotonic time axis.
    """
    padded = dict(values)
    for field, width in (("month", 2), ("day", 2), ("hour", 2), ("minute", 2), ("second", 2)):
        if field in padded and isinstance(padded[field], int):
            padded[field] = f"{padded[field]:0{width}d}"
    if "year" in padded and isinstance(padded["year"], int):
        padded["year"] = f"{padded['year']:04d}"

    missing = set(template_fields(template)) - set(padded)
    if missing:
        raise KeyError(
            f"Path template {template!r} needs fields {sorted(missing)} "
            f"that were not supplied. Got: {sorted(padded)}"
        )
    return Path(template.format(**padded))


def parse_template(template: str, path: str | Path) -> dict[str, str]:
    """Recover the field values a path was rendered from. Inverse of ``render_template``.

    Returns an empty dict when the path doesn't match the template.
    """
    pattern = re.escape(template)
    for field in template_fields(template):
        pattern = pattern.replace(re.escape("{" + field + "}"), f"(?P<{field}>[^/]+)")
    match = re.fullmatch(pattern, str(path))
    return match.groupdict() if match else {}


def atomic_write(target: Path, write: "callable") -> Path:
    """Write via a temporary file and rename into place.

    A crashed 3am cron must never leave a half-written master behind, and
    ``os.replace`` is atomic within a filesystem. ``write`` is called with the
    temporary path.
    """
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp{os.getpid()}")
    try:
        write(tmp)
        os.replace(tmp, target)
    finally:
        if tmp.exists():
            tmp.unlink()
    return target
