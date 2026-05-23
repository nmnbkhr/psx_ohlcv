# Claude Code Prompt: PKISRV/PKRV Full Yield Curve — Discovery & Scraper

## Problem

The app currently has PKISRV rates up to 1Y (12M) only. The full sovereign yield 
curve from MUFAP has **21 tenors**: 1W, 2W, 1M, 2M, 3M, 4M, 6M, 9M, 12M, 2Y, 3Y, 
4Y, 5Y, 6Y, 7Y, 8Y, 9Y, 10Y, 15Y, 20Y, 30Y.

Missing the 2Y–30Y long end means the Yield Curve page, Treasury Terminal, and 
Bond Market page cannot show the full term structure — which is the most important 
part for PIB pricing, duration risk, and macro regime analysis.

**Three data types from MUFAP:**
- **PKRV** — Pakistan Revaluation rates (conventional government curve)
- **PKISRV** — Pakistan Islamic Sovereign Revaluation rates (Shariah-compliant curve)
- **PKFRV** — Pakistan Forward Revaluation rates

## Step 0: Audit What Exists in the Codebase

```bash
cd ~/pakfindata && conda activate psx

# 1. Find ALL references to PKRV, PKISRV, PKFRV
echo "=== ALL PKRV/PKISRV/PKFRV references ==="
grep -rn "PKRV\|PKISRV\|PKFRV\|pkrv\|pkisrv\|pkfrv\|yield.curve\|yield_curve\|revaluation" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__ | grep -v ".pyc"

# 2. Find the source/scraper that fetches PKISRV data
echo "=== Source files ==="
grep -rn "PKRV\|PKISRV\|mufap\|MUFAP\|yield.*rate\|revaluation" \
    ~/pakfindata/src/pakfindata/sources/ --include="*.py" | grep -v __pycache__

# 3. Find the UI page that displays it
echo "=== UI pages ==="
grep -rn "PKRV\|PKISRV\|yield.*curve\|yield_curve\|tenor\|revaluation" \
    ~/pakfindata/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__

# 4. Find the DB table/schema for yield curve data
echo "=== DB schema ==="
grep -rn "PKRV\|PKISRV\|pkrv\|yield.*curve\|revaluation\|tenor" \
    ~/pakfindata/src/pakfindata/db/ --include="*.py" | grep -v __pycache__

# 5. Check SQLite for existing data
echo "=== SQLite tables ==="
sqlite3 /mnt/e/psxdata/psx.sqlite ".tables" | tr ' ' '\n' | grep -i "pkrv\|yield\|rate\|bond\|tenor\|curve"

# 6. Check if there's a PKRV/rates table and what tenors it has
echo "=== Rate tables and columns ==="
for tbl in pkrv_rates pkisrv_rates yield_curve rates sbp_rates daily_rates pkrv pkisrv; do
    sqlite3 /mnt/e/psxdata/psx.sqlite ".schema $tbl" 2>/dev/null
done

# 7. Sample data — what tenors do we currently have?
echo "=== Current tenor data ==="
for tbl in pkrv_rates pkisrv_rates yield_curve; do
    echo "--- $tbl ---"
    sqlite3 /mnt/e/psxdata/psx.sqlite -header -column \
        "SELECT * FROM $tbl ORDER BY rowid DESC LIMIT 5;" 2>/dev/null || echo "table not found"
done

# 8. Check SBP EasyData scraper — does it fetch PKRV?
echo "=== SBP EasyData scraper ==="
grep -rn "PKRV\|yield\|revaluation\|TS_GP" \
    ~/pakfindata/src/pakfindata/sources/sbp*.py --include="*.py" 2>/dev/null | head -20

# 9. Check config for MUFAP URLs
echo "=== Config/constants ==="
grep -rn "mufap\|MUFAP\|pkrv\|PKRV" \
    ~/pakfindata/src/pakfindata/config*.py 2>/dev/null
grep -rn "mufap\|MUFAP\|pkrv\|PKRV" \
    ~/pakfindata/src/pakfindata/constants*.py 2>/dev/null

# 10. Check fi_sync service
echo "=== fi_sync service ==="
grep -rn "PKRV\|PKISRV\|mufap\|yield\|curve" \
    ~/pakfindata/src/pakfindata/services/fi_sync*.py 2>/dev/null | head -20
```

**READ ALL OUTPUT.** Record:
- Which table stores PKRV/PKISRV data
- What tenors currently exist (column names or tenor values)
- Which scraper fetches the data and from what URL
- Which pages display it

## Step 1: Discover MUFAP New Site API

The old MUFAP site (`old.mufap.com.pk`) had CSVs at predictable URLs:
```
https://mufap.com.pk/pdf/PKISRVs/{YYYY}/{Month}/PKISRV{DDMMYYYY}.csv
https://mufap.com.pk/pdf/PKRVs/{YYYY}/{Month}/PKRV{DDMMYYYY}.csv
https://mufap.com.pk/pdf/PKFRVs/{YYYY}/{Month}/PKFRV{DDMMYYYY}.csv
```
Where `{Month}` = January, February, ..., and date = DDMMYYYY.

These are returning 404 now. The new site at `www.mufap.com.pk/WebRegulations/Index` 
loads content via AJAX. We need to find the new endpoints.

### 1a. Try old CSV URLs with various patterns

```bash
# Test if old URLs still work under different domains
echo "=== Testing old CSV URLs ==="

# Pattern: PKISRV{DDMMYYYY}.csv
for url in \
    "https://mufap.com.pk/pdf/PKISRVs/2026/April/PKISRV15042026.csv" \
    "https://www.mufap.com.pk/pdf/PKISRVs/2026/April/PKISRV15042026.csv" \
    "https://mufap.com.pk/pdf/PKISRVs/2026/Apr/PKISRV15042026.csv" \
    "https://mufap.com.pk/pdf/PKISRVs/2025/December/PKISRV31122025.csv" \
    "https://mufap.com.pk/pdf/PKRVs/2026/April/PKRV15042026.csv" \
    "https://www.mufap.com.pk/pdf/PKRVs/2026/April/PKRV15042026.csv" \
    "https://mufap.com.pk/pdf/PKRVs/2025/December/PKRV31122025.csv" \
    "https://mufap.com.pk/pdf/PKRVs/2025/June/PKRV30062025.csv" \
    "https://mufap.com.pk/pdf/PKRVs/2024/December/PKRV31122024.csv" \
    ; do
    status=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)
    echo "$status $url"
done

# Also try lowercase month, Mar vs March, etc.
for url in \
    "https://mufap.com.pk/pdf/PKRVs/2025/june/PKRV30062025.csv" \
    "https://mufap.com.pk/pdf/PKRVs/2025/Jun/PKRV30062025.csv" \
    "https://mufap.com.pk/pdf/PKRVs/2025/JUNE/PKRV30062025.csv" \
    ; do
    status=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)
    echo "$status $url"
done
```

### 1b. If any CSV URL returns 200, download and inspect it

```bash
# Download a working CSV and show its contents
WORKING_URL=""  # Set this to whichever URL returned 200

if [ -n "$WORKING_URL" ]; then
    curl -s "$WORKING_URL" -o /tmp/pkrv_sample.csv
    echo "=== CSV CONTENTS ==="
    cat /tmp/pkrv_sample.csv
    echo ""
    echo "=== COLUMN COUNT ==="
    head -1 /tmp/pkrv_sample.csv | tr ',' '\n' | nl
fi
```

### 1c. Discover new MUFAP AJAX endpoints

```bash
# The new MUFAP site at www.mufap.com.pk loads data via AJAX.
# Look at the page source for API calls.

echo "=== Page source — script tags ==="
curl -s "https://www.mufap.com.pk/WebRegulations/Index?Head=Pricing&title=PKRV/PKISRV/PKFRV" \
    | grep -oP 'src="[^"]*\.js[^"]*"' | head -20

echo "=== Inline scripts ==="
curl -s "https://www.mufap.com.pk/WebRegulations/Index?Head=Pricing&title=PKRV/PKISRV/PKFRV" \
    | grep -oP '<script[^>]*>.*?</script>' | head -10

echo "=== AJAX URLs in source ==="
curl -s "https://www.mufap.com.pk/WebRegulations/Index?Head=Pricing&title=PKRV/PKISRV/PKFRV" \
    | grep -oP 'https?://[^"'\''<>\s]+' | sort -u | grep -i "api\|ajax\|get\|post\|data\|json\|csv\|download\|regulation\|pricing"

echo "=== Data loading patterns ==="
curl -s "https://www.mufap.com.pk/WebRegulations/Index?Head=Pricing&title=PKRV/PKISRV/PKFRV" \
    | grep -oP '(fetch|ajax|XMLHttpRequest|axios|\.get|\.post)\([^)]+\)' | head -10

# Try common ASP.NET / MVC API patterns
echo "=== Testing API patterns ==="
for endpoint in \
    "https://www.mufap.com.pk/WebRegulations/GetRegulations" \
    "https://www.mufap.com.pk/WebRegulations/GetData" \
    "https://www.mufap.com.pk/api/regulations" \
    "https://www.mufap.com.pk/WebRegulations/GetRegulationList" \
    "https://www.mufap.com.pk/WebRegulations/List" \
    ; do
    resp=$(curl -s -o /tmp/mufap_resp.txt -w "%{http_code}" \
        -H "Content-Type: application/json" \
        -H "X-Requested-With: XMLHttpRequest" \
        "$endpoint?Head=Pricing&title=PKRV/PKISRV/PKFRV" 2>/dev/null)
    size=$(wc -c < /tmp/mufap_resp.txt 2>/dev/null || echo 0)
    echo "$resp ${size}B $endpoint"
    if [ "$resp" = "200" ] && [ "$size" -gt 100 ]; then
        echo "  >>> FOUND! First 500 chars:"
        head -c 500 /tmp/mufap_resp.txt
        echo ""
    fi
done

# Try POST variants
for endpoint in \
    "https://www.mufap.com.pk/WebRegulations/GetRegulations" \
    "https://www.mufap.com.pk/WebRegulations/GetData" \
    ; do
    resp=$(curl -s -o /tmp/mufap_resp.txt -w "%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -H "X-Requested-With: XMLHttpRequest" \
        -d '{"Head":"Pricing","title":"PKRV/PKISRV/PKFRV"}' \
        "$endpoint" 2>/dev/null)
    size=$(wc -c < /tmp/mufap_resp.txt 2>/dev/null || echo 0)
    echo "POST $resp ${size}B $endpoint"
    if [ "$resp" = "200" ] && [ "$size" -gt 100 ]; then
        echo "  >>> FOUND! First 500 chars:"
        head -c 500 /tmp/mufap_resp.txt
        echo ""
    fi
done
```

### 1d. Discover the full page JavaScript

```bash
# Download the full HTML and extract all JS file references
curl -s "https://www.mufap.com.pk/WebRegulations/Index?Head=Pricing&title=PKRV/PKISRV/PKFRV" \
    > /tmp/mufap_page.html

echo "=== All script sources ==="
grep -oP 'src="([^"]+)"' /tmp/mufap_page.html | grep -i "\.js"

echo "=== All data-* attributes ==="
grep -oP 'data-[a-z-]+="[^"]*"' /tmp/mufap_page.html | sort -u | head -20

echo "=== Form actions ==="
grep -oP 'action="[^"]*"' /tmp/mufap_page.html

echo "=== Any JSON embedded in the page ==="
python3 -c "
import re
html = open('/tmp/mufap_page.html').read()
# Find JSON-like data
jsons = re.findall(r'\{[^{}]{50,}\}', html)
for j in jsons[:5]:
    print(j[:200])
    print('---')
"

# Download and inspect each JS file
for js_url in $(grep -oP 'src="([^"]+\.js[^"]*)"' /tmp/mufap_page.html | \
    sed 's/src="//;s/"$//' | head -10); do
    
    # Make absolute URL
    if [[ "$js_url" != http* ]]; then
        js_url="https://www.mufap.com.pk${js_url}"
    fi
    
    echo "=== JS: $js_url ==="
    curl -s "$js_url" 2>/dev/null | grep -i "regulation\|pricing\|pkrv\|csv\|download\|ajax\|fetch\|api\|getdata" | head -10
done
```

## Step 2: Discover Khistocks API (Backup Source)

Khistocks (Business Recorder) has PKRV rates at:
`https://www.khistocks.com/market-data/pkrv-rates.html`

The table shows all 21 tenors but data loads via AJAX.

```bash
# Get the khistocks page and find the data endpoint
curl -s "https://www.khistocks.com/market-data/pkrv-rates.html" > /tmp/khi_pkrv.html

echo "=== Script sources ==="
grep -oP 'src="[^"]*\.js[^"]*"' /tmp/khi_pkrv.html | head -20

echo "=== AJAX/API patterns in HTML ==="
grep -oP "(url|ajax|fetch|XMLHttp|api)[^'\"]*['\"][^'\"]+['\"]" /tmp/khi_pkrv.html | head -20

echo "=== data- attributes ==="
grep -oP 'data-[a-z-]+="[^"]*"' /tmp/khi_pkrv.html | sort -u | head -20

# Look at inline scripts for PKRV data loading
python3 -c "
import re
html = open('/tmp/khi_pkrv.html').read()
scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
for i, s in enumerate(scripts):
    if len(s.strip()) > 50:
        if any(kw in s.lower() for kw in ['pkrv', 'rate', 'table', 'ajax', 'fetch', 'api', 'url', 'data']):
            print(f'=== Script {i} ({len(s)} chars) ===')
            print(s[:1000])
            print('---')
"

# Try common khistocks API patterns
echo "=== Testing khistocks APIs ==="
for endpoint in \
    "https://www.khistocks.com/api/pkrv-rates" \
    "https://www.khistocks.com/market-data/pkrv-data.json" \
    "https://www.khistocks.com/ajax/pkrv-rates" \
    "https://www.khistocks.com/market-data/get-pkrv-rates" \
    ; do
    resp=$(curl -s -o /tmp/khi_resp.txt -w "%{http_code}" "$endpoint" 2>/dev/null)
    size=$(wc -c < /tmp/khi_resp.txt 2>/dev/null || echo 0)
    echo "$resp ${size}B $endpoint"
    if [ "$resp" = "200" ] && [ "$size" -gt 50 ]; then
        head -c 300 /tmp/khi_resp.txt
        echo ""
    fi
done
```

## Step 3: Try Old MUFAP Site Direct (Fallback)

The old site at `old.mufap.com.pk` still renders the listing pages. The CSV links 
point to `mufap.com.pk/pdf/...` which may redirect.

```bash
# Check if old site CSVs redirect to new location
echo "=== Follow redirects ==="
curl -sL -o /tmp/pkrv_redirect.csv -w "final_url: %{url_effective}\nhttp_code: %{http_code}\n" \
    "https://mufap.com.pk/pdf/PKRVs/2024/Jan/PKRV02012024.csv" 2>/dev/null

curl -sL -o /tmp/pkisrv_redirect.csv -w "final_url: %{url_effective}\nhttp_code: %{http_code}\n" \
    "https://mufap.com.pk/pdf/PKISRVs/2024/Jan/PKISRV02012024.csv" 2>/dev/null

# Check with www prefix
curl -sL -o /dev/null -w "final_url: %{url_effective}\nhttp_code: %{http_code}\n" \
    "https://www.mufap.com.pk/pdf/PKRVs/2024/Jan/PKRV02012024.csv" 2>/dev/null

# Try 2025 dates
for m in "January" "February" "March" "June" "September" "December"; do
    for d in "02" "15" "28"; do
        mm=$(printf "%02d" $(echo "January February March April May June July August September October November December" | tr ' ' '\n' | grep -n "^${m}$" | cut -d: -f1))
        url="https://mufap.com.pk/pdf/PKRVs/2025/${m}/PKRV${d}${mm}2025.csv"
        status=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)
        if [ "$status" = "200" ]; then
            echo "FOUND: $url"
            curl -s "$url" | head -5
            break 2
        fi
    done
done

# Try 2026 dates
for m in "January" "February" "March" "April"; do
    mm=$(printf "%02d" $(echo "January February March April May June July August September October November December" | tr ' ' '\n' | grep -n "^${m}$" | cut -d: -f1))
    for d in "02" "10" "15"; do
        url="https://mufap.com.pk/pdf/PKRVs/2026/${m}/PKRV${d}${mm}2026.csv"
        status=$(curl -s -o /dev/null -w "%{http_code}" "$url" 2>/dev/null)
        echo "$status $url"
        if [ "$status" = "200" ]; then
            echo "  >>> CONTENTS:"
            curl -s "$url" | head -10
            break 2
        fi
    done
done
```

## Step 4: If CSV Found — Parse and Map Tenors

```bash
# If any CSV was downloaded successfully, parse it
if [ -f /tmp/pkrv_sample.csv ] && [ -s /tmp/pkrv_sample.csv ]; then
    python3 -c "
import csv, io

data = open('/tmp/pkrv_sample.csv').read()
print('=== RAW (first 500 chars) ===')
print(data[:500])
print()

# Try different delimiters
for delim in [',', '\t', '|', ';']:
    reader = csv.reader(io.StringIO(data), delimiter=delim)
    rows = list(reader)
    if len(rows) > 1 and len(rows[0]) > 3:
        print(f'=== Parsed with delimiter \"{delim}\" ===')
        print(f'Rows: {len(rows)}')
        print(f'Columns: {len(rows[0])}')
        print(f'Header: {rows[0]}')
        for r in rows[1:4]:
            print(f'  {r}')
        break
"
fi
```

## Step 5: Compile Discovery Report

Write `PKISRV_PKRV_DISCOVERY_REPORT.md` with:

### 1. Current State in App
- Table name and schema
- Tenors currently stored (list them)
- Source URL the current scraper uses
- Which pages display the data

### 2. MUFAP Data Availability
- Old CSV URL pattern — working or 404?
- New site AJAX endpoints discovered
- CSV format (columns, delimiters, tenor labels)
- Date range available

### 3. Khistocks API
- Endpoint URL
- Response format
- All 21 tenors confirmed?

### 4. Recommended Approach
Based on what was discovered:
- Primary source (MUFAP CSVs if still accessible, or MUFAP AJAX, or khistocks)
- Backup source
- URL pattern for daily scraping
- Tenor mapping: `{"1W": 7, "2W": 14, "1M": 30, "2M": 60, "3M": 91, "4M": 122, "6M": 182, "9M": 274, "12M": 365, "2Y": 730, "3Y": 1095, "4Y": 1460, "5Y": 1825, "6Y": 2190, "7Y": 2555, "8Y": 2920, "9Y": 3285, "10Y": 3650, "15Y": 5475, "20Y": 7300, "30Y": 10950}`

### 5. Schema Change Needed
- Current table: what needs to be added/changed to store all 21 tenors
- Should it be wide (one column per tenor) or tall (date, curve_type, tenor, yield)?

### 6. Integration Points
- Which pages need updating to show 2Y–30Y
- Yield Curve page: extend the chart x-axis
- Treasury Terminal: full curve in Overview tab
- Bond Market: PKRV reference curve overlay
- Macro HMM engine: can use 2Y–10Y spread as regime signal
