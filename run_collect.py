#!/usr/bin/env python3
"""
GitHub Actions에서 실행되는 RSS 수집 스크립트
credentials.json을 환경변수에서 복원 후 수집 실행
"""
import os
import json
import sys

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

    # RSS 수집 실행
    from scheduler import collect_and_save_news
    collect_and_save_news()
    print("✅ RSS 수집 완료")

if __name__ == "__main__":
    main()
