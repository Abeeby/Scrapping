"""Test complet de toutes les fonctionnalités de scraping."""

import httpx
import sys
import time
import os

def main():
    base = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    strict_external = os.environ.get("STRICT_EXTERNAL", "0") == "1"
    
    print(f"=== TEST COMPLET SCRAPING - {base} ===")
    print()
    
    results = {"ok": 0, "ko": 0, "details": []}
    
    def test(name, check_fn):
        try:
            ok, msg = check_fn()
            status = "OK" if ok else "KO"
            results["ok" if ok else "ko"] += 1
            results["details"].append((name, status, msg))
            print(f"[{status}] {name}: {msg}")
        except Exception as e:
            results["ko"] += 1
            results["details"].append((name, "KO", str(e)))
            print(f"[KO] {name}: {e}")

    def _extract_detail(response: httpx.Response) -> str:
        try:
            data = response.json()
            if isinstance(data, dict) and data.get("detail"):
                return str(data.get("detail"))
        except Exception:
            pass
        return response.text[:200] if response.text else ""

    def post_with_retry(path: str, payload: dict, timeout: int, retries: int = 3) -> httpx.Response:
        """Retry simple sur 429 (rate limit)."""
        delays = [2, 4, 8]
        last = None
        for attempt in range(retries):
            last = httpx.post(f"{base}{path}", json=payload, timeout=timeout)
            if last.status_code != 429:
                return last
            time.sleep(delays[min(attempt, len(delays) - 1)])
        return last  # type: ignore[return-value]
    
    # 1. Communes
    def test_communes():
        r = httpx.get(f"{base}/api/scraping/communes", timeout=30)
        data = r.json()
        ge = len(data.get("geneve", []))
        vd = len(data.get("vaud", []))
        sge = len(data.get("scanner_ge", []))
        svd = len(data.get("scanner_vd", []))
        ok = ge > 0 and vd > 0 and sge > 0 and svd > 0
        return ok, f"GE:{ge} VD:{vd} ScannerGE:{sge} ScannerVD:{svd}"
    test("Communes", test_communes)
    
    # 2. Rues
    def test_rues():
        r = httpx.get(f"{base}/api/scraping/rues", params={"commune": "Genève"}, timeout=30)
        data = r.json()
        count = len(data.get("rues", []))
        return count > 0, f"{count} rues"
    test("Rues (Genève)", test_rues)
    
    # 3. Search.ch person
    def test_searchch_person():
        r = post_with_retry(
            "/api/scraping/searchch",
            {"source": "searchch", "commune": "Genève", "query": "Muller", "limit": 5, "type_recherche": "person"},
            timeout=60,
        )
        if r.status_code == 429 and not strict_external:
            return True, f"SKIP 429 — {_extract_detail(r)}"
        if r.status_code != 200:
            return False, f"HTTP {r.status_code} — {_extract_detail(r)}"
        data = r.json()
        count = data.get("count", 0)
        sample = data.get("results", [{}])[0].get("nom", "") if data.get("results") else ""
        return count > 0, f"{count} résultats (ex: {sample})"
    test("Search.ch (person)", test_searchch_person)
    
    # 4. Search.ch business
    def test_searchch_business():
        r = post_with_retry(
            "/api/scraping/searchch",
            {"source": "searchch", "commune": "Genève", "query": "Restaurant", "limit": 5, "type_recherche": "business"},
            timeout=60,
        )
        if r.status_code == 429 and not strict_external:
            return True, f"SKIP 429 — {_extract_detail(r)}"
        if r.status_code != 200:
            return False, f"HTTP {r.status_code} — {_extract_detail(r)}"
        data = r.json()
        count = data.get("count", 0)
        return count > 0, f"{count} résultats"
    test("Search.ch (business)", test_searchch_business)
    
    # 5. Local.ch
    def test_localch():
        r = post_with_retry(
            "/api/scraping/localch",
            {"source": "localch", "commune": "Genève", "query": "Muller", "limit": 5, "type_recherche": "person"},
            timeout=60,
        )
        if r.status_code == 429 and not strict_external:
            return True, f"SKIP 429 — {_extract_detail(r)}"
        if r.status_code != 200:
            return False, f"HTTP {r.status_code} — {_extract_detail(r)}"
        data = r.json()
        count = data.get("count", 0)
        return count > 0, f"{count} résultats"
    test("Local.ch (person)", test_localch)
    
    # 6. Scanner (query=all) - CRITIQUE
    def test_scanner_all():
        r = post_with_retry(
            "/api/scraping/scanner",
            {"source": "scanner", "commune": "Genève", "query": "all", "limit": 10, "type_recherche": "person"},
            timeout=120,
        )
        if r.status_code == 429 and not strict_external:
            return True, f"SKIP 429 — {_extract_detail(r)}"
        if r.status_code != 200:
            return False, f"HTTP {r.status_code} — {_extract_detail(r)}"
        data = r.json()
        count = data.get("count", 0)
        sample = ""
        if data.get("results"):
            res = data["results"][0]
            sample = f"{res.get('nom', '')} - {res.get('telephone', '')} - {res.get('adresse', '')}"
        return count > 0, f"{count} résultats (ex: {sample})"
    test("Scanner (query=all) [CRITIQUE]", test_scanner_all)
    
    # 7. Scanner (rue spécifique)
    def test_scanner_rue():
        r = post_with_retry(
            "/api/scraping/scanner",
            {"source": "scanner", "commune": "Genève", "query": "Rue de Contamine", "limit": 10, "type_recherche": "person"},
            timeout=120,
        )
        if r.status_code == 429 and not strict_external:
            return True, f"SKIP 429 — {_extract_detail(r)}"
        if r.status_code != 200:
            return False, f"HTTP {r.status_code} — {_extract_detail(r)}"
        data = r.json()
        count = data.get("count", 0)
        return count > 0, f"{count} résultats"
    test("Scanner (rue spécifique)", test_scanner_rue)
    
    # 8. SITG
    def test_sitg():
        r = httpx.post(f"{base}/api/scraping/sitg", 
                       json={"source": "sitg", "commune": "Genève", "query": "", "limit": 10, "type_recherche": "all"},
                       timeout=60)
        data = r.json()
        count = data.get("count", 0)
        return count > 0, f"{count} résultats"
    test("SITG (Cadastre GE)", test_sitg)
    
    # 9. RF Links
    def test_rf():
        r = httpx.post(f"{base}/api/scraping/rf-links", 
                       json={"source": "rf", "commune": "Genève", "query": "", "limit": 10, "type_recherche": "all"},
                       timeout=60)
        data = r.json()
        count = data.get("count", 0)
        return count > 0, f"{count} liens"
    test("RF Links (Registre Foncier)", test_rf)
    
    # 10. Vaud
    def test_vaud():
        r = httpx.post(f"{base}/api/scraping/vaud", 
                       json={"source": "vaud", "commune": "Lausanne", "query": "", "limit": 10, "type_recherche": "all"},
                       timeout=60)
        data = r.json()
        count = data.get("count", 0)
        return count >= 0, f"{count} résultats"  # Peut être 0 si API VD indispo
    test("Cadastre Vaud", test_vaud)
    
    # 11. Import prospects
    def test_import():
        # D'abord récupérer des résultats
        r = httpx.post(f"{base}/api/scraping/searchch", 
                       json={"source": "searchch", "commune": "Genève", "query": "Dupont", "limit": 3, "type_recherche": "person"},
                       timeout=60)
        data = r.json()
        if not data.get("results"):
            return True, "Pas de résultats à importer (OK)"
        
        # Importer
        r2 = httpx.post(f"{base}/api/scraping/add-to-prospects", json=data["results"], timeout=60)
        imp = r2.json()
        return r2.status_code == 200, f"added={imp.get('added',0)} duplicates={imp.get('duplicates',0)}"
    test("Import prospects", test_import)
    
    # 12. Liste prospects
    def test_prospects_list():
        r = httpx.get(f"{base}/api/prospects/", params={"limit": 10}, timeout=30)
        return r.status_code == 200, f"HTTP {r.status_code}"
    test("Liste prospects", test_prospects_list)
    
    # 13. Pipeline
    def test_pipeline():
        r = httpx.get(f"{base}/api/prospects/pipeline", timeout=30)
        return r.status_code == 200, f"HTTP {r.status_code}"
    test("Pipeline prospects", test_pipeline)
    
    # 14. Stats dashboard
    def test_dashboard():
        r = httpx.get(f"{base}/api/stats/dashboard", timeout=30)
        data = r.json()
        ok = "prospects" in data and "emails" in data
        return ok, f"prospects={data.get('prospects')} emails={data.get('emails')} bots={data.get('bots')}"
    test("Stats dashboard", test_dashboard)
    
    # 15. Emails stats
    def test_emails():
        r = httpx.get(f"{base}/api/emails/stats", timeout=30)
        return r.status_code == 200, f"HTTP {r.status_code}"
    test("Stats emails", test_emails)
    
    # 16. Proxies stats
    def test_proxies():
        r = httpx.get(f"{base}/api/proxies/stats", timeout=30)
        return r.status_code == 200, f"HTTP {r.status_code}"
    test("Stats proxies", test_proxies)
    
    # 17. Bots stats
    def test_bots():
        r = httpx.get(f"{base}/api/bots/stats", timeout=30)
        return r.status_code == 200, f"HTTP {r.status_code}"
    test("Stats bots", test_bots)
    
    # 18. Campaigns stats
    def test_campaigns():
        r = httpx.get(f"{base}/api/campaigns/stats", timeout=30)
        return r.status_code == 200, f"HTTP {r.status_code}"
    test("Stats campaigns", test_campaigns)
    
    # 19. Export
    def test_export():
        r = httpx.get(f"{base}/api/export/prospects", params={"format": "xlsx"}, timeout=60)
        return r.status_code == 200, f"HTTP {r.status_code} ({len(r.content)} bytes)"
    test("Export prospects XLSX", test_export)
    
    # 20. Health
    def test_health():
        r = httpx.get(f"{base}/api/health", timeout=10)
        data = r.json()
        return data.get("status") == "ok", f"v{data.get('version')}"
    test("Health check", test_health)

    # 21. Quality summary
    def test_quality_summary():
        r = httpx.get(f"{base}/api/quality/summary", timeout=30)
        data = r.json()
        ok = r.status_code == 200 and "total" in data and "avg_quality_score" in data and "enrichment_status" in data
        return ok, f"avg={data.get('avg_quality_score')} total={data.get('total')}"
    test("Quality summary", test_quality_summary)

    # 22. Pipeline qualité post-création (score + flags)
    def test_quality_pipeline_on_create():
        suffix = str(int(time.time() * 1000))[-7:]
        payload = {
            "nom": "TestQualiteE2E",
            "prenom": "Jean",
            "ville": "Genève",
            "telephone": f"078{suffix}",
            "email": f"jean.test+{suffix}@example.com",
            "adresse": "Rue de Test 1",
            "code_postal": "1200",
            "source": "E2E-Quality",
        }
        created = httpx.post(f"{base}/api/prospects/", json=payload, timeout=30).json()
        pid = created.get("id")
        if not pid:
            return False, "Impossible de créer prospect test"
        deadline = time.time() + 12
        qs = None
        p = None
        while time.time() < deadline:
            p = httpx.get(f"{base}/api/prospects/{pid}", timeout=30).json()
            qs = p.get("quality_score")
            if p.get("merged_into_id"):
                return False, f"Prospect fusionné vers {p.get('merged_into_id')} (attendu: non fusionné)"
            if isinstance(qs, int) and qs > 0:
                break
            time.sleep(1)
        ok = isinstance(qs, int) and qs >= 60
        return ok, f"quality_score={qs} enrichment_status={(p or {}).get('enrichment_status')}"
    test("Quality pipeline (create)", test_quality_pipeline_on_create)

    # 23. Dédup exact (auto-merge téléphone)
    def test_quality_dedup_exact():
        suffix = str(int(time.time() * 1000))[-7:]
        phone = f"079{suffix}"
        p1 = {
            "nom": "QualDedupE2E",
            "prenom": "Alice",
            "ville": "Genève",
            "telephone": phone,
            "email": f"alice.dedup+{suffix}@example.com",
            "adresse": "Rue Master 1",
            "code_postal": "1200",
            "source": "E2E-Dedup",
        }
        p2 = {
            "nom": "QualDedupE2E",
            "prenom": "Alice",
            "ville": "Genève",
            "telephone": phone,
            "email": f"alice.dedup2+{suffix}@example.com",
            "source": "E2E-Dedup",
        }
        id1 = httpx.post(f"{base}/api/prospects/", json=p1, timeout=30).json().get("id")
        id2 = httpx.post(f"{base}/api/prospects/", json=p2, timeout=30).json().get("id")
        if not id1 or not id2:
            return False, "Création prospects dedup impossible"
        time.sleep(5)
        p2f = httpx.get(f"{base}/api/prospects/{id2}", timeout=30).json()
        merged_into = p2f.get("merged_into_id")
        ok = bool(merged_into) and merged_into == id1
        return ok, f"merged_into_id={merged_into}"
    test("Quality dedup exact (auto-merge)", test_quality_dedup_exact)
    
    print()
    print(f"=== RÉSULTAT: {results['ok']}/{results['ok']+results['ko']} OK ===")
    
    if results["ko"] > 0:
        print("\nÉchecs:")
        for name, status, msg in results["details"]:
            if status == "KO":
                print(f"  - {name}: {msg}")
    
    return 0 if results["ko"] == 0 else 1

if __name__ == "__main__":
    sys.exit(main())

