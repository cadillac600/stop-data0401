"""
Kabutan ストップ高・ストップ安 スクレイパー
毎日 16:00 JST 以降に実行する
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import time
import sys
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))
DATA_FILE = "data/stock_data.json"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def scrape_kabutan(mode: str) -> list[dict]:
    """
    mode='3_1' → ストップ高
    mode='3_2' → ストップ安
    """
    url = f"https://kabutan.jp/warning/?mode={mode}"
    print(f"  Fetching {url}")

    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    resp.encoding = "utf-8"

    soup = BeautifulSoup(resp.text, "html.parser")
    stocks = []
    seen_codes = set()

    # 銘柄コードのリンクを起点にして行を特定
    for link in soup.select('a[href*="/stock/?code="]'):
        code = link.get_text(strip=True)
        if not code or code in seen_codes:
            continue

        row = link.find_parent("tr")
        if not row:
            continue

        cells = row.find_all("td")
        if len(cells) < 6:
            continue

        seen_codes.add(code)

        def cell_text(i):
            return cells[i].get_text(strip=True) if i < len(cells) else ""

        name       = cell_text(1)
        market     = cell_text(2)
        price_raw  = cell_text(3).replace(",", "")
        change_raw = cell_text(4).replace(",", "")
        rate_raw   = cell_text(5).replace(",", "").replace("%", "")

        # PER / PBR / 利回り（あれば）
        per = cell_text(7).replace(",", "") if len(cells) > 7 else ""
        pbr = cell_text(8).replace(",", "") if len(cells) > 8 else ""

        stocks.append({
            "code":   code,
            "name":   name,
            "market": market,
            "price":  price_raw,
            "change": change_raw,
            "rate":   rate_raw,
            "per":    per,
            "pbr":    pbr,
        })

    return stocks


def load_existing() -> list[dict]:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save(all_data: list[dict]) -> None:
    os.makedirs("data", exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)


def main():
    now = datetime.now(JST)
    date_str = now.strftime("%Y-%m-%d")
    print(f"=== 株データ取得: {date_str} ===")

    try:
        print("ストップ高 取得中...")
        stop_high = scrape_kabutan("3_1")
        print(f"  → {len(stop_high)} 銘柄")

        time.sleep(2)

        print("ストップ安 取得中...")
        stop_low = scrape_kabutan("3_2")
        print(f"  → {len(stop_low)} 銘柄")

    except requests.RequestException as e:
        print(f"エラー: スクレイピング失敗 - {e}", file=sys.stderr)
        sys.exit(1)

    today_record = {
        "date":       date_str,
        "updated_at": now.isoformat(),
        "stop_high":  stop_high,
        "stop_low":   stop_low,
    }

    all_data = load_existing()
    # 同日のデータがあれば上書き
    all_data = [d for d in all_data if d.get("date") != date_str]
    all_data.append(today_record)
    # 日付降順、最大90日分
    all_data.sort(key=lambda x: x["date"], reverse=True)
    all_data = all_data[:90]

    save(all_data)
    print(f"完了: {DATA_FILE} に保存しました")


if __name__ == "__main__":
    main()
