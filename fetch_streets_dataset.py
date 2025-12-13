"""Génère `app/data/streets.json` à partir d’un backend déployé.

Pourquoi:
- Le Scanner Quartier dépend de `streets.json`.
- Ce script permet de (re)constituer le dataset depuis un environnement prod/staging.

Usage:
  python fetch_streets_dataset.py --base-url https://web-production-269f3.up.railway.app

Par défaut:
- Sortie: backend/app/data/streets.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Dict, List

import httpx


async def fetch_json(client: httpx.AsyncClient, url: str, params=None):
    r = await client.get(url, params=params)
    r.raise_for_status()
    return r.json()


async def fetch_rues_for_commune(client: httpx.AsyncClient, base_url: str, commune: str, sem: asyncio.Semaphore):
    async with sem:
        data = await fetch_json(client, f"{base_url}/api/scraping/rues", params={"commune": commune})
        rues = data.get("rues") or []
        return commune, rues


async def run(base_url: str, out_path: str, concurrency: int) -> int:
    base_url = base_url.rstrip("/")

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        communes = await fetch_json(client, f"{base_url}/api/scraping/communes")

        scanner_ge: List[str] = communes.get("scanner_ge") or []
        scanner_vd: List[str] = communes.get("scanner_vd") or []

        if not scanner_ge and not scanner_vd:
            raise RuntimeError("Impossible de récupérer scanner_ge/scanner_vd depuis /api/scraping/communes")

        sem = asyncio.Semaphore(concurrency)

        ge_tasks = [fetch_rues_for_commune(client, base_url, c, sem) for c in scanner_ge]
        vd_tasks = [fetch_rues_for_commune(client, base_url, c, sem) for c in scanner_vd]

        print(f"Communes GE: {len(scanner_ge)} | VD: {len(scanner_vd)}")

        ge_pairs = await asyncio.gather(*ge_tasks)
        vd_pairs = await asyncio.gather(*vd_tasks)

    dataset: Dict[str, Dict[str, List[str]]] = {
        "GE": {commune: rues for commune, rues in ge_pairs},
        "VD": {commune: rues for commune, rues in vd_pairs},
    }

    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(dataset, f, ensure_ascii=False, indent=2, sort_keys=True)

    ge_non_empty = sum(1 for rues in dataset["GE"].values() if rues)
    vd_non_empty = sum(1 for rues in dataset["VD"].values() if rues)
    print(f"Écrit: {out_path}")
    print(f"Communes avec rues (GE): {ge_non_empty}/{len(scanner_ge)} | (VD): {vd_non_empty}/{len(scanner_vd)}")

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch streets dataset depuis un backend déployé")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("PROSPECTIONPRO_BASE_URL", "https://web-production-269f3.up.railway.app"),
        help="URL de base du backend (Railway/Vercel/etc)",
    )
    parser.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "app", "data", "streets.json"),
        help="Chemin de sortie streets.json",
    )
    parser.add_argument("--concurrency", type=int, default=10, help="Nombre de requêtes concurrentes")

    args = parser.parse_args()

    raise SystemExit(asyncio.run(run(args.base_url, args.out, args.concurrency)))


if __name__ == "__main__":
    main()

