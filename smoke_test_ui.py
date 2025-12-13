"""Smoke tests UI (déploiement) pour ProspectionPro.

Objectif:
- Vérifier que le frontend (SPA) est bien servi par le backend.
- Vérifier que les routes principales renvoient bien le HTML (catch-all).
- Vérifier que les assets référencés dans index.html existent.
- Vérifier un handshake Socket.IO minimal.

Ces tests ne simulent pas des clics UI; ils valident le *déploiement web*.
"""

from __future__ import annotations

import argparse
import os
import re
from typing import List, Tuple

import httpx

from audit_matrix import UI_ROUTES


def _normalize_base_url(base_url: str) -> str:
    return base_url.strip().rstrip("/")


def _get(client: httpx.Client, base_url: str, path: str, **kwargs) -> httpx.Response:
    if not path.startswith("/"):
        path = "/" + path
    return client.get(base_url + path, **kwargs)


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _extract_assets(html: str) -> List[str]:
    # Vite: <script type="module" crossorigin src="/assets/index-XXXX.js"></script>
    #       <link rel="stylesheet" crossorigin href="/assets/index-XXXX.css">
    return sorted(set(re.findall(r"/assets/[A-Za-z0-9_.-]+\.(?:js|css|svg)", html)))


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke tests UI ProspectionPro")
    parser.add_argument(
        "--base-url",
        default=os.environ.get("PROSPECTIONPRO_BASE_URL", "http://127.0.0.1:8000"),
        help="URL de base (ex: https://xxx.up.railway.app)",
    )

    args = parser.parse_args()
    base_url = _normalize_base_url(args.base_url)

    failures: List[str] = []

    try:
        with httpx.Client(follow_redirects=True, timeout=60) as client:
            # 1) index.html
            r = _get(client, base_url, "/")
            _assert(r.status_code == 200, f"GET / -> HTTP {r.status_code}")
            html = r.text
            _assert("id=\"root\"" in html or "id='root'" in html, "index.html ne contient pas #root")

            # 2) assets
            assets = _extract_assets(html)
            _assert(len(assets) > 0, "Aucun asset /assets/... trouvé dans index.html")

            for asset in assets:
                ra = _get(client, base_url, asset)
                _assert(ra.status_code == 200, f"GET {asset} -> HTTP {ra.status_code}")

            # 3) routes SPA
            for route in [u["route"] for u in UI_ROUTES]:
                rr = _get(client, base_url, route)
                _assert(rr.status_code == 200, f"GET {route} -> HTTP {rr.status_code}")
                _assert(
                    "id=\"root\"" in rr.text or "id='root'" in rr.text,
                    f"Route {route} ne renvoie pas le HTML SPA",
                )

            # 4) Socket.IO handshake minimal (polling)
            # Note: on ne valide pas le contenu finement, juste la disponibilité.
            rs = _get(client, base_url, "/socket.io/", params={"EIO": 4, "transport": "polling", "t": "smoke"})
            _assert(rs.status_code == 200, f"GET /socket.io polling -> HTTP {rs.status_code}")

    except Exception as e:
        failures.append(str(e))

    if failures:
        print(f"Base URL: {base_url}")
        print("KO")
        for f in failures:
            print(f"- {f}")
        raise SystemExit(1)

    print(f"Base URL: {base_url}")
    print("OK")


if __name__ == "__main__":
    main()
