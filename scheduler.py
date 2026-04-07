#!/usr/bin/env python3
"""
스케줄러 모듈
주기적으로 뉴스를 수집하고 구글 시트에 저장
"""

import schedule
import time
import threading
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo('Asia/Seoul')
except ImportError:
    KST = timezone(timedelta(hours=9))

from global_alpha_reader import collect_rss_feeds, get_recent_news
from google_sheets_archive import get_sheets_archive, GSPREAD_AVAILABLE
from gemini_summarizer import summarize_news

MAX_RETRIES = 2

def collect_and_save_news():
    """뉴스 수집 및 구글 시트 저장 (재시도 포함)"""
    for attempt in range(MAX_RETRIES + 1):
        try:
            kst_now = datetime.now(KST)
            timestamp = kst_now.strftime('%Y-%m-%d %H:%M:%S KST')
            print(f"\n{'='*60}")
            print(f"🔄 자동 수집 시작 ({timestamp}){f' [재시도 {attempt}]' if attempt > 0 else ''}")
            print('='*60)

            # RSS 피드 수집
            news_items = collect_rss_feeds(use_sheets=True)

            if news_items:
                print(f"✅ {len(news_items)}개 뉴스 수집 완료")

                # 구글 시트에서 최신 뉴스 확인
                if GSPREAD_AVAILABLE:
                    try:
                        sheets = get_sheets_archive()
                        recent_count = len(sheets.get_recent_news(limit=1000, days=1))
                        print(f"📊 구글 시트에 총 {recent_count}개 뉴스 저장됨 (T/T-1)")
                    except Exception as e:
                        print(f"⚠️ 구글 시트 확인 오류: {e}")
            else:
                print("⚠️ 수집된 뉴스가 없습니다.")

            print(f"{'='*60}\n")
            return  # 성공 시 리턴

        except Exception as e:
            print(f"❌ 자동 수집 오류 (시도 {attempt+1}/{MAX_RETRIES+1}): {e}")
            import traceback
            traceback.print_exc()
            if attempt < MAX_RETRIES:
                wait = 30 * (attempt + 1)
                print(f"⏳ {wait}초 후 재시도...")
                time.sleep(wait)

def run_scheduler(interval_minutes: int = 5):
    """스케줄러 실행"""
    print(f"⏰ 스케줄러 시작: {interval_minutes}분마다 실행")
    
    # 즉시 한 번 실행
    collect_and_save_news()
    
    # 주기적 실행 설정
    schedule.every(interval_minutes).minutes.do(collect_and_save_news)
    
    # 스케줄러 실행 루프
    while True:
        schedule.run_pending()
        time.sleep(60)  # 1분마다 체크

def start_scheduler_background(interval_minutes: int = 5):
    """백그라운드에서 스케줄러 시작"""
    def scheduler_thread():
        run_scheduler(interval_minutes)
    
    thread = threading.Thread(target=scheduler_thread, daemon=True)
    thread.start()
    return thread

if __name__ == "__main__":
    import sys
    
    # 명령줄 인자로 간격 설정 (기본값: 5분)
    interval = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    
    print("=" * 60)
    print("🚀 Global Alpha Reader - 자동 수집 스케줄러")
    print("=" * 60)
    print(f"⏰ 수집 간격: {interval}분")
    print(f"📊 구글 시트: https://docs.google.com/spreadsheets/d/1QJwGGenMzgdSxLJeMjk_AQQtUTYUF4E6j64XdHnPLC8")
    print("=" * 60)
    print("\n💡 종료하려면 Ctrl+C를 누르세요\n")
    
    try:
        run_scheduler(interval)
    except KeyboardInterrupt:
        print("\n\n✅ 스케줄러 종료")
