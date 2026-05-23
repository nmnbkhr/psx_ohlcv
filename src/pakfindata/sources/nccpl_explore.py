"""NCCPL page structure explorer.

Run this BEFORE writing any parser to understand what the NCCPL pages
actually look like — table headers, column order, date pickers, etc.

Uses Playwright (bundled Chromium) to bypass Cloudflare challenge.

Usage:
    conda activate psx
    python -m pakfindata.sources.nccpl_explore
"""

import logging
import json

log = logging.getLogger("pakfindata.nccpl_explore")

URLS = {
    "fipi": "https://www.nccpl.com.pk/en/market-information/fipi-lipi/fipi",
    "fipi_daily": "https://www.nccpl.com.pk/en/market-information/fipi-lipi/fipi-normal-daily",
    "fipi_sector": "https://www.nccpl.com.pk/en/portfolio-investments/fipi-sector-wise",
    "lipi": "https://www.nccpl.com.pk/en/market-information/fipi-lipi/lipi",
}


def explore_page(page, name: str, url: str) -> dict:
    """Navigate to a URL and dump its structure."""
    print(f"\n{'='*70}")
    print(f"  EXPLORING: {name}")
    print(f"  URL: {url}")
    print(f"{'='*70}")

    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    # Wait for Cloudflare challenge to resolve + JS render
    page.wait_for_timeout(6000)

    title = page.title()
    print(f"\nTitle: {title}")

    # If still on Cloudflare challenge page, wait longer
    if "moment" in title.lower() or "challenge" in title.lower():
        print("  Still on Cloudflare challenge, waiting 10s more...")
        page.wait_for_timeout(10000)
        title = page.title()
        print(f"  Title after wait: {title}")

    result = {"title": title, "tables": [], "inputs": [], "selects": [], "buttons": []}

    # ── Tables ──
    tables = page.query_selector_all("table")
    print(f"\nFound {len(tables)} table(s)")
    for i, t in enumerate(tables):
        print(f"\n--- TABLE {i} ---")
        headers = t.query_selector_all("th")
        header_texts = [h.inner_text().strip() for h in headers]
        print(f"  Headers ({len(header_texts)}): {header_texts}")

        rows = t.query_selector_all("tr")
        print(f"  Rows: {len(rows)}")
        table_data = {"headers": header_texts, "sample_rows": [], "row_count": len(rows)}

        for r in rows[:6]:
            cells = r.query_selector_all("td")
            cell_texts = [td.inner_text().strip() for td in cells]
            if cell_texts:
                print(f"  Row: {cell_texts}")
                table_data["sample_rows"].append(cell_texts)

        result["tables"].append(table_data)

    # ── Input fields (date pickers etc) ──
    inputs = page.query_selector_all("input")
    print(f"\nFound {len(inputs)} input(s)")
    for inp in inputs:
        attrs = {}
        for attr in ["type", "name", "id", "class", "placeholder", "value"]:
            val = inp.get_attribute(attr)
            if val:
                attrs[attr] = val
        print(f"  Input: {attrs}")
        result["inputs"].append(attrs)

    # ── Select dropdowns ──
    selects = page.query_selector_all("select")
    print(f"\nFound {len(selects)} select(s)")
    for sel in selects:
        sel_id = sel.get_attribute("id") or sel.get_attribute("name") or "?"
        options = sel.query_selector_all("option")
        option_data = [(o.get_attribute("value"), o.inner_text().strip()) for o in options[:15]]
        print(f"  Select '{sel_id}': {option_data}")
        result["selects"].append({"id": sel_id, "options": option_data})

    # ── Buttons ──
    buttons = page.query_selector_all("button")
    print(f"\nFound {len(buttons)} button(s)")
    for btn in buttons:
        btn_text = btn.inner_text().strip()
        btn_type = btn.get_attribute("type")
        print(f"  Button: '{btn_text}' (type={btn_type})")
        result["buttons"].append({"text": btn_text, "type": btn_type})

    # ── Links with submit-like text ──
    for text in ["Search", "Go", "Submit", "Filter"]:
        els = page.query_selector_all(f"text={text}")
        if els:
            print(f"\n  Elements with text '{text}': {len(els)}")

    return result


def main():
    from playwright.sync_api import sync_playwright

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print("NCCPL Page Structure Explorer (Playwright)")
    print("=" * 70)

    results = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = context.new_page()

        for name, url in URLS.items():
            try:
                results[name] = explore_page(page, name, url)
            except Exception as e:
                print(f"\n  ERROR exploring {name}: {e}")
                results[name] = {"error": str(e)}

        browser.close()

    print("\n" + "=" * 70)
    print("  EXPLORATION COMPLETE")
    print("=" * 70)

    # Summary
    print("\nSummary:")
    for name, r in results.items():
        if "error" in r:
            print(f"  {name}: ERROR — {r['error']}")
        else:
            n_tables = len(r.get("tables", []))
            n_inputs = len(r.get("inputs", []))
            print(f"  {name}: {n_tables} table(s), {n_inputs} input(s)")

    # Save raw results for reference
    out_path = "/tmp/nccpl_explore_results.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nFull results saved to {out_path}")

    return results


if __name__ == "__main__":
    main()
