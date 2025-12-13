"""Test all API endpoints on Railway."""
import httpx
import asyncio
import json

BASE_URL = "https://web-production-269f3.up.railway.app"

async def test_all():
    async with httpx.AsyncClient(timeout=30.0) as client:
        endpoints = [
            # Health
            ("GET", "/api/health", None),
            # Prospection
            ("GET", "/api/prospection/sources", None),
            ("GET", "/api/prospection/jobs", None),
            ("GET", "/api/prospection/legal-sources", None),
            ("GET", "/api/prospection/match/stats", None),
            ("GET", "/api/prospection/opendata/search?q=geneve&rows=2", None),
            ("GET", "/api/prospection/rf/communes/GE", None),
            ("GET", "/api/prospection/streets/GE", None),
            # Quality
            ("GET", "/api/quality/summary", None),
            # Biens
            ("GET", "/api/biens/", None),
            ("GET", "/api/biens/stats", None),
            # Brochures
            ("GET", "/api/brochures/pipeline/stats", None),
            # Prospects
            ("GET", "/api/prospects/", None),
            # Match single address
            ("POST", "/api/prospection/match", {"adresse": "Rue du Rhone 1", "code_postal": "1204", "ville": "Geneve"}),
        ]
        
        print("=" * 70)
        print("TESTING RAILWAY API ENDPOINTS")
        print("=" * 70)
        
        results = {"ok": [], "fail": []}
        
        for method, path, body in endpoints:
            url = f"{BASE_URL}{path}"
            try:
                if method == "GET":
                    r = await client.get(url)
                else:
                    r = await client.post(url, json=body)
                
                status = "OK" if r.status_code in (200, 201) else "FAIL"
                
                if status == "OK":
                    results["ok"].append(path)
                    print(f"[OK]   {method} {path} -> {r.status_code}")
                else:
                    results["fail"].append((path, r.status_code, r.text[:200]))
                    print(f"[FAIL] {method} {path} -> {r.status_code}")
                    print(f"       Response: {r.text[:150]}")
                    
            except Exception as e:
                results["fail"].append((path, 0, str(e)))
                print(f"[ERR]  {method} {path} -> {type(e).__name__}: {str(e)[:100]}")
        
        print("\n" + "=" * 70)
        print(f"SUMMARY: {len(results['ok'])} OK, {len(results['fail'])} FAIL")
        print("=" * 70)
        
        if results["fail"]:
            print("\nFailed endpoints:")
            for path, code, msg in results["fail"]:
                print(f"  - {path}: {code}")

if __name__ == "__main__":
    asyncio.run(test_all())
