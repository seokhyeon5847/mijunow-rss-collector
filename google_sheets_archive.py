#!/usr/bin/env python3
"""
구글 시트 아카이브 모듈
뉴스를 구글 시트에 저장하여 빠른 접근 및 아카이빙
"""

try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GSPREAD_AVAILABLE = True
except ImportError:
    GSPREAD_AVAILABLE = False
    print("⚠️ gspread 또는 oauth2client가 설치되지 않았습니다.")
    print("💡 설치: pip3 install gspread oauth2client")

import json
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import hashlib
import time
from data_cleaner import clean_news_items, filter_noise, check_duplicate_by_title

# pytz 대신 zoneinfo 사용 (Python 3.9+)
try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo('Asia/Seoul')
except ImportError:
    # Python 3.8 이하에서는 UTC+9 시간대 직접 계산
    KST = timezone(timedelta(hours=9))

# 구글 시트 설정
SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]

import os
from dotenv import load_dotenv
load_dotenv()
CREDENTIALS_FILE = os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json')
# 구글 시트 ID
SPREADSHEET_ID = os.getenv('GOOGLE_SHEETS_ID', '1QJwGGenMzgdSxLJeMjk_AQQtUTYUF4E6j64XdHnPLC8')
SPREADSHEET_NAME = 'Global Alpha Reader News Archive'  # 구글 시트 이름 (폴백용)

# 시트 구조 (확장)
COLUMNS = {
    'date': 'A',      # 날짜
    'time': 'B',      # 시간
    'site': 'C',      # 사이트명
    'title': 'D',     # 뉴스 제목
    'link': 'E',      # 원문 링크 (RSS 피드에서 실제 링크)
    'summary': 'F',   # Gemini 요약문
    'category': 'G',  # 카테고리 태그 (Macro, Tech, Biotech, Earnings 등)
    'tickers': 'H',   # 티커/연관 티커 (예: AAPL,MSFT,TSLA)
    'priority': 'I',  # 중요도 점수 (1-10)
    'analyzed': 'J',  # 분석 여부 (TRUE: 시장 분석에 사용됨, FALSE: 미사용)
    'buzz_score': 'K' # 실시간 화제성 점수 (1-10) — 배치 내 중복 보도 기반
}

class GoogleSheetsArchive:
    """구글 시트 아카이브 클래스"""
    
    def __init__(self):
        """초기화 및 구글 시트 연결"""
        self.client = None
        self.spreadsheet = None
        self.worksheet = None
        self._connect()
    
    def _connect(self):
        """구글 시트 연결"""
        if not GSPREAD_AVAILABLE:
            raise ImportError("gspread 또는 oauth2client가 설치되지 않았습니다.")
        
        try:
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(f"{CREDENTIALS_FILE} 파일이 없습니다.")
            
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                CREDENTIALS_FILE, SCOPE
            )
            self.client = gspread.authorize(credentials)
            
            # 시트 ID로 직접 열기 (우선)
            try:
                self.spreadsheet = self.client.open_by_key(SPREADSHEET_ID)
                print(f"✅ 구글 시트 열기 성공 (ID: {SPREADSHEET_ID})")
            except Exception as e:
                print(f"⚠️ 시트 ID로 열기 실패: {e}, 이름으로 시도...")
                try:
                    self.spreadsheet = self.client.open(SPREADSHEET_NAME)
                except gspread.SpreadsheetNotFound:
                    print(f"⚠️ 시트를 찾을 수 없습니다. 새로 생성합니다...")
                    self.spreadsheet = self.client.create(SPREADSHEET_NAME)
                    # 시트 공유 (서비스 계정 이메일)
                    self.spreadsheet.share('id-486@gen-lang-client-0094915354.iam.gserviceaccount.com', perm_type='user', role='writer')
            
            # 날짜별 탭 가져오기 또는 생성
            self.worksheet = self._get_or_create_date_tab()
            
            # 헤더 설정 (없으면 추가)
            self._setup_headers()
            
            print(f"✅ 구글 시트 연결 완료: {SPREADSHEET_NAME}")
            
        except Exception as e:
            print(f"⚠️ 구글 시트 연결 실패: {e}")
            raise
    
    def _get_kst_now(self) -> datetime:
        """한국 시간(KST) 현재 시간 반환"""
        return datetime.now(KST)
    
    def _get_date_tab_name(self, dt: datetime = None) -> str:
        """날짜별 탭 이름 생성 (YYYY-MM-DD 형식)"""
        if dt is None:
            dt = self._get_kst_now()
        return dt.strftime('%Y-%m-%d')
    
    def _should_use_previous_day(self) -> bool:
        """현재 시간이 한국 시간 12:00 PM 이전인지 확인"""
        kst_now = self._get_kst_now()
        return kst_now.hour < 12
    
    def _get_or_create_date_tab(self):
        """날짜별 탭 가져오기 또는 생성"""
        try:
            # 한국 시간 기준 날짜 결정
            kst_now = self._get_kst_now()
            
            # 12시 이전이면 전날 탭 사용, 이후면 오늘 탭 사용
            if self._should_use_previous_day():
                target_date = kst_now - timedelta(days=1)
            else:
                target_date = kst_now
            
            tab_name = self._get_date_tab_name(target_date)
            
            # 기존 탭 찾기
            try:
                worksheet = self.spreadsheet.worksheet(tab_name)
                print(f"✅ 기존 탭 사용: {tab_name}")
                return worksheet
            except gspread.WorksheetNotFound:
                # 새 탭 생성
                print(f"📅 새 탭 생성: {tab_name}")
                worksheet = self.spreadsheet.add_worksheet(
                    title=tab_name,
                    rows=10000,
                    cols=10
                )
                return worksheet
        except Exception as e:
            print(f"⚠️ 날짜별 탭 생성 오류: {e}, 기본 시트 사용")
            try:
                return self.spreadsheet.sheet1
            except:
                return self.spreadsheet.add_worksheet(title="News Archive", rows=10000, cols=10)
    
    def _setup_headers(self):
        """시트 헤더 설정"""
        try:
            # 첫 번째 행 확인
            header_row = self.worksheet.row_values(1)
            
            if not header_row or len(header_row) < 11:
                # 헤더 추가 (확장된 구조 — buzz_score 포함)
                headers = [
                    '날짜',           # A열
                    '시간',           # B열
                    '사이트명',       # C열
                    '뉴스 제목',      # D열
                    '원문 링크',      # E열 (RSS 피드 실제 링크)
                    'Gemini 요약문',  # F열 (한국어)
                    '카테고리',       # G열
                    '티커/연관 티커', # H열 (예: AAPL,MSFT,TSLA)
                    '중요도',         # I열 (1-10점)
                    '분석 여부',      # J열 (TRUE/FALSE)
                    '화제성'          # K열 (buzz_score, 1-10점)
                ]
                self.worksheet.insert_row(headers, 1)
                print("✅ 시트 헤더 설정 완료")
        except Exception as e:
            print(f"⚠️ 헤더 설정 오류: {e}")
    
    def generate_hash(self, title: str, link: str) -> str:
        """뉴스 아이템의 고유 해시 생성 (중복 제거용)"""
        combined = f"{title}|{link}"
        return hashlib.md5(combined.encode()).hexdigest()
    
    def is_duplicate(self, title: str, link: str, check_similarity: bool = True) -> bool:
        """
        중복 뉴스 확인
        - 링크 일치 확인
        - 제목 유사도 확인 (90% 이상)
        """
        try:
            # 링크 컬럼에서 확인 (E열)
            all_links = self.worksheet.col_values(5)  # E열 (링크)
            all_titles = self.worksheet.col_values(4)  # D열 (제목)
            
            # 1. 링크 일치 확인
            for existing_link in all_links[1:]:  # 헤더 제외
                if existing_link == link:
                    return True
            
            # 2. 제목 유사도 확인 (90% 이상)
            if check_similarity:
                existing_titles = all_titles[1:]  # 헤더 제외
                if check_duplicate_by_title(title, existing_titles, threshold=0.9):
                    return True
            
            return False
        except Exception as e:
            print(f"⚠️ 중복 확인 오류: {e}")
            return False
    
    def add_news(self, news_items: List[Dict], summaries: Optional[Dict[str, Dict]] = None, target_date: Optional[datetime] = None):
        """
        뉴스 추가 (배치 처리, 최적화)
        summaries: {link: {'summary': str, 'category': str, 'priority': int}} 형태
        target_date: 특정 날짜 탭에 저장하려면 datetime 객체 전달 (None이면 현재 날짜 기준)
        """
        if not news_items:
            return
        
        # 날짜별 탭 가져오기 (target_date가 지정되면 해당 날짜 사용)
        if target_date:
            tab_name = self._get_date_tab_name(target_date)
            try:
                self.worksheet = self.spreadsheet.worksheet(tab_name)
            except:
                self.worksheet = self.spreadsheet.add_worksheet(
                    title=tab_name,
                    rows=10000,
                    cols=10
                )
                self._setup_headers()
        else:
            # 날짜별 탭 다시 가져오기 (날짜가 바뀌었을 수 있음)
            self.worksheet = self._get_or_create_date_tab()
        
        summaries = summaries or {}
        new_rows = []
        added_count = 0
        duplicate_count = 0
        filtered_count = 0
        
        # 중복 확인을 위한 캐시 (성능 향상)
        try:
            all_links = set(self.worksheet.col_values(5)[1:])  # E열 (링크), 헤더 제외
            all_titles = self.worksheet.col_values(4)[1:]  # D열 (제목)
            title_link_pairs = set(zip(all_titles, all_links))
        except:
            all_links = set()
            all_titles = []
            title_link_pairs = set()
        
        # 데이터 정리 (노이즈 필터링, 중복 제거)
        cleaned_items = clean_news_items(news_items, existing_titles=all_titles)
        filtered_count = len(news_items) - len(cleaned_items)
        
        for news in cleaned_items:
            title = news.get('title', '').strip()
            link = news.get('link', '').strip()
            site = news.get('site', '')
            summary = news.get('summary', '').strip()
            
            if not title or not link:
                continue
            
            # 노이즈 필터링 (이미 clean_news_items에서 처리했지만 재확인)
            if filter_noise(title, summary):
                filtered_count += 1
                continue
            
            # 빠른 중복 확인 (캐시 사용)
            if (title, link) in title_link_pairs:
                duplicate_count += 1
                continue
            
            # 상세 중복 확인 (링크 + 제목 유사도)
            if self.is_duplicate(title, link, check_similarity=True):
                duplicate_count += 1
                title_link_pairs.add((title, link))
                continue
            
            # Gemini 요약문 및 메타데이터 (있으면 사용)
            summary_data = summaries.get(link, {})
            gemini_summary = summary_data.get('summary', summary)
            category = summary_data.get('category', '')
            priority = summary_data.get('priority', '')
            tickers = summary_data.get('tickers', '')
            buzz_score = summary_data.get('buzz_score', '5')
            
            # 티커가 없으면 제목과 요약에서 추출
            if not tickers:
                from stock_tickers import extract_tickers_from_text
                combined_text = f"{title} {summary}"
                extracted_tickers = extract_tickers_from_text(combined_text)
                if extracted_tickers:
                    tickers = ','.join(extracted_tickers[:5])  # 최대 5개까지
            
            # 요약문이 너무 길면 자르기
            if len(gemini_summary) > 500:
                gemini_summary = gemini_summary[:500] + "..."
            
            # 날짜/시간 (published_date 우선, 없으면 현재 시간)
            published_date = news.get('published_date', '')
            if published_date:
                try:
                    # published_date에서 날짜/시간 추출
                    if ' ' in published_date:
                        date_part, time_part = published_date.split(' ', 1)
                        date_str = date_part
                        time_str = time_part.split()[0] if time_part else '00:00:00'
                    else:
                        date_str = published_date
                        time_str = '00:00:00'
                except:
                    kst_now = self._get_kst_now()
                    date_str = kst_now.strftime('%Y-%m-%d')
                    time_str = kst_now.strftime('%H:%M:%S')
            else:
                kst_now = self._get_kst_now()
                date_str = kst_now.strftime('%Y-%m-%d')
                time_str = kst_now.strftime('%H:%M:%S')
            
            # 분석 여부 (기본값 FALSE)
            analyzed = 'FALSE'
            
            # 행 데이터 (확장된 구조)
            row_data = [
                date_str,      # A열: 날짜
                time_str,      # B열: 시간
                site,          # C열: 사이트명
                title,         # D열: 뉴스 제목
                link,          # E열: 원문 링크 (RSS 피드 실제 링크)
                gemini_summary, # F열: Gemini 요약문 (한국어)
                category,      # G열: 카테고리 태그
                tickers,       # H열: 티커/연관 티커
                priority,      # I열: 중요도 점수
                analyzed,      # J열: 분석 여부
                buzz_score     # K열: 실시간 화제성 점수 (1-10)
            ]
            
            new_rows.append(row_data)
            title_link_pairs.add((title, link))
            all_titles.append(title)
            all_links.add(link)
            added_count += 1
        
        # 배치 추가 (한 번에 여러 행 추가, 최대 100개씩)
        if new_rows:
            try:
                # 큰 배치를 작은 배치로 나누기 (API 제한 방지)
                batch_size = 200
                for i in range(0, len(new_rows), batch_size):
                    batch = new_rows[i:i+batch_size]
                    self.worksheet.append_rows(batch)
                    print(f"✅ 구글 시트에 {len(batch)}개 뉴스 추가 ({i+1}/{len(new_rows)})")
                    time.sleep(0.2)  # API 제한 방지
                
                print(f"✅ 구글 시트에 총 {added_count}개 뉴스 추가 완료 (노이즈 {filtered_count}개, 중복 {duplicate_count}개 제외)")
            except Exception as e:
                print(f"⚠️ 구글 시트 배치 추가 오류: {e}, 개별 추가로 시도...")
                # 개별 추가로 폴백
                for row in new_rows[:50]:  # 최대 50개만 시도
                    try:
                        self.worksheet.append_row(row)
                        time.sleep(0.1)
                    except Exception as e2:
                        print(f"⚠️ 개별 추가 실패: {e2}")
                        break
    
    def get_recent_news(self, limit: int = 200, days: int = 1) -> List[Dict]:
        """최근 뉴스 가져오기 (T와 T-1만, KST 기준)"""
        try:
            # 여러 날짜 탭에서 데이터 수집
            all_news_items = []
            kst_now = self._get_kst_now()

            # T와 T-1만 조회 (days=1이면 오늘+어제)
            for day_offset in range(days + 1):
                target_date = kst_now - timedelta(days=day_offset)
                tab_name = self._get_date_tab_name(target_date)
                
                try:
                    worksheet = self.spreadsheet.worksheet(tab_name)
                    all_values = worksheet.get_all_values()
                    
                    if len(all_values) <= 1:  # 헤더만 있으면
                        continue
                    
                    # 헤더 제외하고 데이터만
                    data_rows = all_values[1:]
                    
                    for row in reversed(data_rows):  # 최신순
                        if len(row) < 5:
                            continue
                        
                        # 헤더 행 건너뛰기
                        if row[0] == '날짜' or row[0] == 'Date' or (row[3] and row[3] == '뉴스 제목'):
                            continue
                        
                        # 열 인덱스 확인 (A=0, B=1, C=2, D=3, E=4, F=5, G=6, H=7, I=8, J=9)
                        date_str = row[0] if len(row) > 0 else ''
                        time_str = row[1] if len(row) > 1 else ''
                        site = row[2] if len(row) > 2 else ''
                        title = row[3] if len(row) > 3 else ''
                        link = row[4] if len(row) > 4 else ''
                        summary = row[5] if len(row) > 5 else ''
                        category = row[6] if len(row) > 6 else ''
                        
                        # H열과 I열 확인: 티커가 비어있고 중요도가 H열에 있는 경우
                        h_col = row[7] if len(row) > 7 else ''
                        i_col = row[8] if len(row) > 8 else ''
                        
                        # H열이 숫자면 중요도, I열이 TRUE/FALSE면 분석 여부
                        if h_col.isdigit():
                            priority = h_col  # H열이 중요도
                            analyzed = i_col if i_col in ['TRUE', 'FALSE'] else 'FALSE'  # I열이 분석 여부
                            tickers = ''  # 티커 데이터 없음
                        else:
                            tickers = h_col  # H열이 티커
                            priority = i_col if i_col.isdigit() else ''  # I열이 중요도
                            analyzed = row[9] if len(row) > 9 and row[9] in ['TRUE', 'FALSE'] else 'FALSE'  # J열이 분석 여부
                        
                        if not title or not link or title == '뉴스 제목':
                            continue
                        
                        # 중요도 필터링 (5 이상만 - app_alpha.py와 일치)
                        try:
                            priority_int = int(priority) if priority and priority.isdigit() else 0
                            if priority_int < 5:
                                continue
                        except:
                            # 중요도가 없거나 숫자가 아니면 건너뛰기
                            continue
                        
                        all_news_items.append({
                            'site': site,
                            'title': title,
                            'summary': summary or title,
                            'link': link,
                            'category': category,
                            'tickers': tickers,  # 티커 데이터 포함
                            'priority': priority,
                            'published_date': f"{date_str} {time_str}" if date_str else datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'collected_date': f"{date_str} {time_str}" if date_str else datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'analyzed': analyzed == 'TRUE'
                        })
                        
                        if len(all_news_items) >= limit:
                            break
                    
                    if len(all_news_items) >= limit:
                        break
                        
                except gspread.WorksheetNotFound:
                    # 해당 날짜 탭이 없으면 건너뛰기
                    continue
                except Exception as e:
                    print(f"⚠️ 탭 {tab_name} 읽기 오류: {e}")
                    continue
            
            return all_news_items[:limit]
            
        except Exception as e:
            print(f"⚠️ 구글 시트에서 뉴스 가져오기 오류: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def update_summary(self, link: str, summary: str):
        """특정 뉴스의 요약문 업데이트"""
        try:
            all_links = self.worksheet.col_values(5)  # E열 (링크)
            
            for i, existing_link in enumerate(all_links[1:], start=2):
                if existing_link == link:
                    # F열 업데이트 (요약문)
                    self.worksheet.update_cell(i, 6, summary)
                    # G열 업데이트 (분석 여부)
                    self.worksheet.update_cell(i, 7, 'TRUE')
                    return True
            
            return False
        except Exception as e:
            print(f"⚠️ 요약문 업데이트 오류: {e}")
            return False

# 전역 인스턴스
_sheets_archive = None

def get_sheets_archive() -> GoogleSheetsArchive:
    """구글 시트 아카이브 인스턴스 가져오기 (싱글톤)"""
    global _sheets_archive
    if _sheets_archive is None:
        _sheets_archive = GoogleSheetsArchive()
    return _sheets_archive
