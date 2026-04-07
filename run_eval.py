#!/usr/bin/env python3
"""
Claude 스케줄러에서 실행되는 뉴스 평가 스크립트
Claude가 직접 Google Sheets의 미평가 뉴스를 읽고 평가/점수를 매김
(Gemini API 사용하지 않음 — Claude 자체 판단)
credentials.json은 환경변수에서 복원
"""
import os
import json
import time

# credentials.json 복원
creds = os.environ.get('GOOGLE_CREDENTIALS_JSON')
if creds:
    with open('credentials.json', 'w') as f:
        f.write(creds)
    print("✅ credentials.json 복원 완료")
else:
    print("❌ GOOGLE_CREDENTIALS_JSON 환경변수 없음")
    exit(1)

import gspread
from oauth2client.service_account import ServiceAccountCredentials

SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
SPREADSHEET_ID = '1QJwGGenMzgdSxLJeMjk_AQQtUTYUF4E6j64XdHnPLC8'

from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo('Asia/Seoul')
except ImportError:
    KST = timezone(timedelta(hours=9))


def get_sheet_client():
    credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', SCOPE)
    client = gspread.authorize(credentials)
    return client.open_by_key(SPREADSHEET_ID)


def get_unrated_news(worksheet):
    """미평가 뉴스 찾기 (I열=중요도 비어있는 행)"""
    all_rows = worksheet.get_all_values()
    if len(all_rows) <= 1:
        return []
    unrated = []
    for row_idx, row in enumerate(all_rows[1:], start=2):
        importance = row[8] if len(row) > 8 else ''
        if not importance or importance.strip() == '':
            title = row[3] if len(row) > 3 else ''
            link = row[4] if len(row) > 4 else ''
            site = row[2] if len(row) > 2 else ''
            if title:
                unrated.append({
                    'row_idx': row_idx,
                    'title': title,
                    'link': link,
                    'site': site,
                })
    return unrated


def print_unrated_for_claude(tab_name, unrated):
    """Claude가 읽고 평가할 수 있도록 미평가 뉴스 출력"""
    print(f"\n{'='*60}")
    print(f"📋 탭: {tab_name} — 미평가 뉴스 {len(unrated)}개")
    print(f"{'='*60}")
    for i, news in enumerate(unrated, 1):
        print(f"\n[{i}] 행 {news['row_idx']}")
        print(f"  사이트: {news['site']}")
        print(f"  제목: {news['title']}")
    print(f"\n{'='*60}")


def main():
    print("=" * 60)
    print("🧠 미주나우 뉴스 평가 — 미평가 뉴스 조회")
    print("=" * 60)

    spreadsheet = get_sheet_client()
    kst_now = datetime.now(KST)
    tabs_to_check = [
        kst_now.strftime('%Y-%m-%d'),
        (kst_now - timedelta(days=1)).strftime('%Y-%m-%d'),
    ]

    total_unrated = 0

    for tab_name in tabs_to_check:
        try:
            worksheet = spreadsheet.worksheet(tab_name)
        except Exception:
            print(f"⚠️ 탭 {tab_name} 없음, 건너뜀")
            continue

        unrated = get_unrated_news(worksheet)
        if not unrated:
            print(f"✅ {tab_name}: 미평가 뉴스 없음")
            continue

        print_unrated_for_claude(tab_name, unrated)
        total_unrated += len(unrated)

    if total_unrated == 0:
        print("\n✅ 모든 뉴스 평가 완료!")
    else:
        print(f"\n📝 총 {total_unrated}개 뉴스 평가 필요")
        print("Claude가 위 뉴스들을 분석하고 Google Sheets에 평가를 업데이트합니다.")


if __name__ == "__main__":
    main()
