"""Smoke tests API pour ProspectionPro.

Objectif:
- Tester rapidement les endpoints critiques (statut, schéma minimal, erreurs visibles).

Usage:
  python smoke_test_api.py --base-url https://... 
  python smoke_test_api.py --base-url http://127.0.0.1:8000 --category scraping

Note:
- S’appuie sur la matrice dans audit_matrix.py.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx

from audit_matrix import API_CASES, ApiCase


@dataclass
class CaseResult:
    case_id: str
    ok: bool
    status_code: Optional[int] = None
    error: Optional[str] = None


def _normalize_base_url(base_url: str) -> str:
    base_url = base_url.strip().rstrip("/")
    return base_url


def _build_url(base_url: str, path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return base_url + path


def _check_non_empty_payload(case: ApiCase, payload: Any) -> Optional[str]:
    """Retourne une erreur si allow_empty=False et que payload est vide."""
    if case.allow_empty:
        return None

    # Cas list
    if isinstance(payload, list):
        if len(payload) == 0:
            return "Réponse vide (liste) alors que allow_empty=False"
        return None

    # Cas dict
    if isinstance(payload, dict):
        # Heuristiques: certains endpoints répondent {results:[...]} ou {rues:[...]}
        for key in ("results", "rues"):
            if key in payload and isinstance(payload[key], list) and len(payload[key]) == 0:
                return f"Réponse vide ({key}=[]) alors que allow_empty=False"
        # Sinon, dict vide
        if len(payload) == 0:
            return "Réponse vide (dict) alors que allow_empty=False"
        return None

    # Autre
    return None


async def _run_case(client: httpx.AsyncClient, base_url: str, case: ApiCase) -> CaseResult:
    url = _build_url(base_url, case.path)

    # Retry léger sur 429 (rate limit) pour les tests scraping
    response = None
    for attempt in range(3):
        try:
            response = await client.request(
                case.method,
                url,
                params=case.query,
                json=case.json,
            )
        except Exception as e:
            return CaseResult(case_id=case.id, ok=False, error=f"Erreur réseau: {e}")

        if response.status_code != 429:
            break
        # backoff: 1s, 2s, 4s
        await asyncio.sleep([1, 2, 4][attempt])

    assert response is not None

    if response.status_code != case.expect_status:
        # Essayer d’extraire un message d’erreur JSON si présent
        detail = None
        try:
            detail = response.json().get("detail")
        except Exception:
            detail = None
        msg = f"HTTP {response.status_code} (attendu {case.expect_status})"
        if detail:
            msg += f" — detail: {detail}"
        return CaseResult(case_id=case.id, ok=False, status_code=response.status_code, error=msg)

    # Validation JSON minimale si attendue
    if case.expect_json_keys is not None:
        try:
            payload = response.json()
        except Exception as e:
            return CaseResult(
                case_id=case.id,
                ok=False,
                status_code=response.status_code,
                error=f"Réponse non-JSON alors que JSON attendu: {e}",
            )

        if not isinstance(payload, dict):
            return CaseResult(
                case_id=case.id,
                ok=False,
                status_code=response.status_code,
                error=f"JSON inattendu: type={type(payload).__name__} (dict attendu)",
            )

        missing = [k for k in case.expect_json_keys if k not in payload]
        if missing:
            return CaseResult(
                case_id=case.id,
                ok=False,
                status_code=response.status_code,
                error=f"Clés manquantes: {missing}",
            )

        non_empty_error = _check_non_empty_payload(case, payload)
        if non_empty_error:
            return CaseResult(
                case_id=case.id,
                ok=False,
                status_code=response.status_code,
                error=non_empty_error,
            )

    return CaseResult(case_id=case.id, ok=True, status_code=response.status_code)


async def run(base_url: str, category: Optional[str], only_id: Optional[str]) -> int:
    base_url = _normalize_base_url(base_url)

    cases = API_CASES
    if category:
        cases = [c for c in cases if c.category == category]
    if only_id:
        cases = [c for c in cases if c.id == only_id]

    if not cases:
        print("Aucun cas à exécuter (filtre trop restrictif).")
        return 2

    async with httpx.AsyncClient(follow_redirects=True, timeout=60) as client:
        results = await asyncio.gather(*[_run_case(client, base_url, c) for c in cases])

    failed = [r for r in results if not r.ok]

    print(f"Base URL: {base_url}")
    print(f"Cas exécutés: {len(results)} | OK: {len(results) - len(failed)} | KO: {len(failed)}")

    if failed:
        print("\nDétails des échecs:")
        for r in failed:
            print(f"- {r.case_id}: {r.error}")
        return 1

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke tests API ProspectionPro")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("PROSPECTIONPRO_BASE_URL", "http://127.0.0.1:8000"),
        help="URL de base (ex: https://xxx.up.railway.app)",
    )
    parser.add_argument(
        "--category",
        default=None,
        help="Filtrer par catégorie (infra, dashboard, scraping, prospects, emails, proxies, bots, campaigns, export)",
    )
    parser.add_argument("--id", dest="only_id", default=None, help="Exécuter un seul cas (par id)")

    args = parser.parse_args()

    code = asyncio.run(run(args.base_url, args.category, args.only_id))
    raise SystemExit(code)


if __name__ == "__main__":
    main()

