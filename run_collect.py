#!/usr/bin/env python3
"""
GitHub Actions에서 실행되는 RSS 수집 스크립트
- RSS 수집 + Google Translate 번역만 수행 (무료)
- Gemini 평가/점수는 로컬에서 별도 실행
"""
import os
import json
import sys
import time

def setup_credentials():
    """GitHub Secrets에서 credentials.json 복원"""
    creds = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    if creds:
        with open('credentials.json', 'w') as f:
            f.write(creds)
        print("✅ credentials.json 복원 완료")
    else:
        print("⚠️ GOOGLE_CREDENTIALS_JSON 환경변수 없음 - Google Sheets 저장 불가")

def main():
    setup_credentials()

    from global_alpha_reader import collect_rss_feeds
    from translator import translate_news_batch

    # 1) RSS 수집 (Gemini 없이, Sheets 저장도 직접 제어)
    print("📡 RSS 피드 수집 중...")
    news_items = collect_rss_feeds(use_sheets=False)
    print(f"✅ {len(news_items)}개 뉴스 수집 완료")

    if not news_items:
        print("⚠️ 수집된 뉴스 없음, 종료")
        return

    # 2) Google Translate로 무료 번역
    print("🌐 Google Translate 번역 중...")
    translated = translate_news_batch(news_items, max_items=len(news_items))
    translated_count = sum(1 for n in translated if n.get('title_kr') and n['title_kr'] != n.get('title'))
    print(f"✅ {translated_count}개 뉴스 번역 완료")

    # 3) Google Sheets에 저장 (Gemini 요약 없이)
    try:
        from google_sheets_archive import get_sheets_archive, GSPREAD_AVAILABLE
        from data_cleaner import clean_news_items
        if GSPREAD_AVAILABLE:
            print("📊 구글 시트에 저장 중...")
            sheets = get_sheets_archive()

            # 데이터 정리 (노이즈 필터링, 중복 제거)
            cleaned_news = clean_news_items(translated)
            print(f"🧹 {len(cleaned_news)}개 뉴스 정리 완료 (제거: {len(translated) - len(cleaned_news)}개)")

            # Gemini 요약 없이 저장 (카테고리/점수는 나중에 로컬에서 채움)
            sheets.add_news(cleaned_news, summaries={})
            print("✅ 구글 시트 저장 완료 (번역만, 평가는 로컬에서)")
        else:
            print("⚠️ gspread 미설치, 구글 시트 저장 건너뜀")
    except Exception as e:
        print(f"⚠️ 구글 시트 저장 실패: {e}")
        import traceback
        traceback.print_exc()

    print("✅ RSS 수집+번역 완료")

if __name__ == "__main__":
    main()
