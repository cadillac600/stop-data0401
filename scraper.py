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

    株探のテーブル構造（table.stock_table）:
    <td class="tac"> コード          ← tds[0]
    <th class="tal"> 銘柄名          ← th要素（tdではない！）
    <td class="tac"> 市場            ← tds[1]
    <td class="gaiyou_icon"> 概要    ← tds[2] スキップ
    <td class="chart_icon">  チャート ← tds[3] スキップ
    <td>             株価            ← tds[4]
    <td>             Sフラグ         ← tds[5] スキップ
    <td class="w61"> 前日比          ← tds[6]
    <td class="w50"> 変動率%         ← tds[7]
    <td class="news_icon"> ニュース  ← tds[8] スキップ
    <td>             PER             ← tds[9]
    <td>             PBR             ← tds[10]
    <td>             利回り          ← tds[11]
    """
    url = f"https://kabutan.jp/warning/?mode={mode}"
    print(f"  Fetching {url}")

    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    resp.encoding = "utf-8"

    soup = BeautifulSoup(resp.text, "html.parser")

    # メインテーブルを特定（class="stock_table"）
    table = soup.find("table", class_="stock_table")
    if not table:
        print("  警告: stock_table が見つかりません")
        return []

    tbody = table.find("tbody")
    if not tbody:
        return []

    stocks = []

    for row in tbody.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 10:
            continue

        # コード（td[0]のリンクテキスト）
        code_link = tds[0].find("a")
        if not code_link:
            continue
        code = code_link.get_text(strip=True)
        if not code:
            continue

        # 銘柄名（th要素 class="tal"）
        name_th = row.find("th", class_="tal")
        name = name_th.get_text(strip=True) if name_th else ""

        # 市場（td[1] class="tac"）
        market = tds[1].get_text(strip=True)

        # 株価（td[4]）
        price = tds[4].get_text(strip=True).replace(",", "")

        # 前日比（td[6] class="w61"）
        change = tds[6].get_text(strip=True).replace(",", "")

        # 変動率（td[7] class="w50"、末尾の % を除去）
        rate = tds[7].get_text(strip=True).replace("%", "").strip().lstrip("+")

        # PER / PBR（td[9] / td[10]、「－」は空文字に）
        def clean_val(td):
            v = td.get_text(strip=True).replace(",", "")
            return "" if "－" in v or v == "-" else v

        per = clean_val(tds[9])
        pbr = clean_val(tds[10])

        stocks.append({
            "code":   code,
            "name":   name,
            "market": market,
            "price":  price,
            "change": change,
            "rate":   rate,
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
