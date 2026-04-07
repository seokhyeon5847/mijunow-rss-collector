#!/usr/bin/env python3
"""
Claude 스케줄러에서 실행되는 뉴스 평가 스크립트
Google Sheets에서 미평가 뉴스를 찾아 Gemini로 평가/점수 매기기
credentials.json은 환경변수에서 복원
"""
import os
import json
import time
import re

# credentials.json 복원
creds = os.environ.get('GOOGLE_CREDENTIALS_JSON')
if creds:
    with open('credentials.json', 'w') as f:
        f.write(creds)
    print("✅ credentials.json 복원 완료")
else:
    print("❌ GOOGLE_CREDENTIALS_JSON 환경변수 없음")
    exit(1)

import google.generativeai as genai
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')
if not GEMINI_API_KEY:
    print("❌ GEMINI_API_KEY 환경변수 없음")
    exit(1)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

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
    all_rows = worksheet.get_all_values()
    if len(all_rows) <= 1:
        return []
    unrated = []
    for row_idx, row in enumerate(all_rows[1:], start=2):
        importance = row[8] if len(row) > 8 else ''
        if not importance or importance.strip() == '':
            title = row[3] if len(row) > 3 else ''
            link = row[4] if len(row) > 4 else ''
            if title:
                unrated.append({
                    'row_idx': row_idx,
                    'title': title,
                    'link': link,
                })
    return unrated


def evaluate_batch(news_batch):
    batch_text = ""
    for i, news in enumerate(news_batch, 1):
        batch_text += f"\n[뉴스 {i}]\n제목: {news['title']}\n"

    prompt = f"""당신은 글로벌 금융 뉴스 큐레이터입니다.

## 출력 형식 (각 기사마다)
1. summary_kr: 한국어 요약 (2-3문장, 팩트 중심)
2. category: 카테고리 (13종 중 택1: Macro, Policy, Tech, Energy, Finance, Earnings, Commodities, Biotech, RealEstate, Crypto, Retail, SEC, Other)
3. tickers: 관련 종목 티커 (없으면 빈 문자열)
4. importance: 뉴스 중요도 (1-10)
5. buzz_score: 실시간 화제성 점수 (1-10)

## 중요도 채점 기준
10점: 시장 전체를 움직이는 이벤트 (연준 금리, 지정학 사건)
9점: 빅테크 실적, 대규모 M&A
8점: 주요 경제지표, 중요 인사 발언
7점: 개별 기업 공시, 업종 동향
5-6점: 일반 시장 뉴스
1-4점: 관련성 낮은 뉴스

---
{batch_text}
---

반드시 유효한 JSON 배열만 출력:
[
  {{"index": 1, "summary_kr": "요약", "category": "카테고리", "tickers": "AAPL", "importance": 7, "buzz_score": 6}}
]"""

    try:
        response = model.generate_content(prompt)
        result_text = response.text.strip()
        json_match = re.search(r'\[[\s\S]*\]', result_text)
        if json_match:
            return json.loads(json_match.group())
    except Exception as e:
        print(f"⚠️ Gemini 평가 실패: {e}")
    return []


def update_sheet_batch(worksheet, updates):
    """배치로 시트 업데이트 (API 호출 절약)"""
    for row_idx, eval_data in updates:
        try:
            worksheet.update_cell(row_idx, 6, eval_data.get('summary_kr', ''))
            worksheet.update_cell(row_idx, 7, eval_data.get('category', 'Other'))
            worksheet.update_cell(row_idx, 8, eval_data.get('tickers', ''))
            worksheet.update_cell(row_idx, 9, str(eval_data.get('importance', 5)))
            worksheet.update_cell(row_idx, 11, str(eval_data.get('buzz_score', 5)))
            time.sleep(0.3)
        except Exception as e:
            print(f"⚠️ 시트 업데이트 실패 (행 {row_idx}): {e}")


def main():
    print("=" * 60)
    print("🧠 Claude 스케줄러 — 뉴스 평가 시작")
    print("=" * 60)

    spreadsheet = get_sheet_client()
    kst_now = datetime.now(KST)
    tabs_to_check = [
        kst_now.strftime('%Y-%m-%d'),
        (kst_now - timedelta(days=1)).strftime('%Y-%m-%d'),
    ]

    total_evaluated = 0

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

        print(f"📋 {tab_name}: {len(unrated)}개 미평가 뉴스 발견")

        BATCH_SIZE = 10
        for batch_start in range(0, len(unrated), BATCH_SIZE):
            batch = unrated[batch_start:batch_start + BATCH_SIZE]
            print(f"  🤖 평가 중... ({batch_start + 1}~{batch_start + len(batch)}/{len(unrated)})")

            results = evaluate_batch(batch)
            updates = []
            for item in results:
                idx = item.get('index', 0) - 1
                if 0 <= idx < len(batch):
                    updates.append((batch[idx]['row_idx'], item))
                    total_evaluated += 1

            if updates:
                update_sheet_batch(worksheet, updates)

            time.sleep(1)

    print(f"\n{'=' * 60}")
    print(f"✅ 완료: {total_evaluated}개 뉴스 평가됨")
    print("=" * 60)


if __name__ == "__main__":
    main()
