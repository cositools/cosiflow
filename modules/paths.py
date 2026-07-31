from __future__ import annotations
import os
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, Tuple, Iterable

# === Config ===
DATA_ROOT = Path(os.getenv("COSIFLOW_DATA_ROOT", "cosi/data"))
YYYY_MM_RE = re.compile(r"^(?P<year>\d{4})_(?P<month>0[1-9]|1[0-2])$")

# === Enums ===
class Domain(str, Enum):
    obs = "obs"
    transient = "transient"
    trigger = "tdrss"
    maps = "maps"
    source = "source"

class ObsLeaf(str, Enum):
    auxil = "auxil"
    compton = "compton"
    acs = "acs"
    bto = "bto"

class CommonLeaf(str, Enum):
    plots = "plots"
    products = "products"

# === Dataclass ===
@dataclass(frozen=True)
class PathInfo:
    domain: Domain
    year: Optional[int] = None
    month: Optional[int] = None
    entity_id: Optional[str] = None
    leaf: Optional[str] = None
    remainder: Tuple[str, ...] = ()

# === Helpers ===
def _ym(year: int, month: int) -> str:
    if not (1 <= month <= 12):
        raise ValueError(f"Invalid month: {month}")
    return f"{year:04d}_{month:02d}"

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def list_files(p: Path, glob: str = "*") -> Iterable[Path]:
    return p.glob(glob)

# === Builder specifici ===
def obs_leaf_path(year: int, month: int, obs_id: str, leaf: ObsLeaf, *rel: str) -> Path:
    return DATA_ROOT / "obs" / _ym(year, month) / obs_id / leaf.value / Path(*rel)

def transient_path(year: int, month: int, transient_id: str, leaf: CommonLeaf, *rel: str) -> Path:
    return DATA_ROOT / "transient" / _ym(year, month) / transient_id / leaf.value / Path(*rel)

def trigger_path(year: int, month: int, trigger_id: str, leaf: CommonLeaf, *rel: str) -> Path:
    return DATA_ROOT / Domain.trigger.value / _ym(year, month) / trigger_id / leaf.value / Path(*rel)

def maps_path(year: int, month: int, *rel: str) -> Path:
    return DATA_ROOT / "maps" / _ym(year, month) / Path(*rel)

def source_path(src_id: str, year: int, month: int, leaf: CommonLeaf, *rel: str) -> Path:
    return DATA_ROOT / "source" / src_id / _ym(year, month) / leaf.value / Path(*rel)

# === Builder generico ===
def build_path(
    domain: Domain,
    *,
    year: Optional[int] = None,
    month: Optional[int] = None,
    entity_id: Optional[str] = None,
    leaf: Optional[str] = None,
    rel: Tuple[str, ...] = (),
) -> Path:
    """Costruisce un path conforme allo schema della figura."""
    if domain == Domain.obs:
        if None in (year, month, entity_id, leaf):
            raise ValueError("obs requires year, month, obs_id and leaf")
        return obs_leaf_path(year, month, entity_id, ObsLeaf(leaf), *rel)

    if domain in (Domain.transient, Domain.trigger):
        if None in (year, month, entity_id, leaf):
            raise ValueError(f"{domain.value} requires year, month, id and leaf")
        fn = transient_path if domain == Domain.transient else trigger_path
        return fn(year, month, entity_id, CommonLeaf(leaf), *rel)

    if domain == Domain.maps:
        if None in (year, month):
            raise ValueError("maps requires year and month")
        return maps_path(year, month, *rel)

    if domain == Domain.source:
        if None in (entity_id, year, month, leaf):
            raise ValueError("source requires src_id, year, month, leaf")
        return source_path(entity_id, year, month, CommonLeaf(leaf), *rel)

    raise ValueError(f"Unsupported domain: {domain}")

# === Parser ===
def parse_path(p: Path) -> PathInfo:
    """Interpreta un path e restituisce un PathInfo con dominio, anno, mese, id e leaf."""
    parts = p.parts[p.parts.index("data")+1:] if "data" in p.parts else p.parts
    domain = Domain(parts[0])

    if domain == Domain.obs:
        ym, entity_id, leaf, *rem = parts[1:]
        m = YYYY_MM_RE.match(ym)
        return PathInfo(domain, int(m["year"]), int(m["month"]), entity_id, leaf, tuple(rem))

    if domain in (Domain.transient, Domain.trigger):
        ym, entity_id, leaf, *rem = parts[1:]
        m = YYYY_MM_RE.match(ym)
        return PathInfo(domain, int(m["year"]), int(m["month"]), entity_id, leaf, tuple(rem))

    if domain == Domain.maps:
        ym, *rem = parts[1:]
        m = YYYY_MM_RE.match(ym)
        return PathInfo(domain, int(m["year"]), int(m["month"]), None, None, tuple(rem))

    if domain == Domain.source:
        src_id, ym, leaf, *rem = parts[1:]
        m = YYYY_MM_RE.match(ym)
        return PathInfo(domain, int(m["year"]), int(m["month"]), src_id, leaf, tuple(rem))

    raise ValueError(f"Unsupported domain: {domain}")

# === Utility extra ===
def file_path(domain: Domain, *, year=None, month=None, entity_id=None, leaf=None, filename: str) -> Path:
    """Restituisce il path completo di un file con nome arbitrario."""
    dirpath = build_path(domain, year=year, month=month, entity_id=entity_id, leaf=leaf)
    ensure_dir(dirpath)
    return dirpath / filename

def first_match(domain: Domain, *, year=None, month=None, entity_id=None, leaf=None, pattern: str = "*") -> Optional[Path]:
    """Restituisce il primo file che combacia con un pattern nella directory canonica."""
    dirpath = build_path(domain, year=year, month=month, entity_id=entity_id, leaf=leaf)
    return next(dirpath.glob(pattern), None)

def route_dest_for(src: Path, domain: Domain, *, year=None, month=None, entity_id=None, leaf=None) -> Path:
    """Costruisce il percorso di destinazione mantenendo il nome originale del file."""
    dirpath = build_path(domain, year=year, month=month, entity_id=entity_id, leaf=leaf)
    ensure_dir(dirpath)
    return dirpath / src.name
