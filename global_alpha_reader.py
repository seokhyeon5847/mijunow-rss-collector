#!/usr/bin/env python3
"""
Global Alpha Reader - 미국 주식 뉴스 원문 직독 플랫폼
원문 중심성과 의사결정 독립성을 위한 뉴스 애그리게이션 플랫폼
"""

import feedparser
import ssl
from datetime import datetime, timedelta, timezone
import hashlib
import sqlite3
import os
from typing import List, Dict, Optional
import re
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
import threading
import requests

# SSL 인증서 검증 우회
ssl._create_default_https_context = ssl._create_unverified_context
feedparser.USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# ==========================================
# 광고/칼럼/의견 필터링 정규표현식 (팩트 기반 뉴스만 수집)
# ==========================================
NOISE_PATTERNS = re.compile(
    r'(sponsored\s*content|advertisement|advertorial|'
    r'opinion:|editorial:|commentary:|columnist|'
    r'follow\s+us|subscribe\s+now|팔로우|구독\s*하세요|'
    r'newsletter|sign\s+up\s+for|'
    r'\bAD\b|\bSPONSORED\b|'
    r'칼럼\s*[:：]|개인\s*의견|투자\s*권유|'
    # 구독/프로모션 유도
    r'exclusive\s+offer|limited\s+time|free\s+trial|premium\s+access|'
    r'unlock\s+full|become\s+a\s+member|join\s+now|'
    r'subscribe\s+to|get\s+started\s+for|discount\s+code|'
    r'무료\s*체험|프리미엄\s*구독|지금\s*가입|'
    # 제휴/광고성 콘텐츠
    r'affiliate|partner\s*content|paid\s*post|brand\s*partner|'
    r'brought\s+to\s+you\s+by|in\s+partnership\s+with|'
    r'제휴|협찬|광고\s*문의|'
    # 의견/칼럼/분석가 주관
    r'my\s+pick|top\s+pick|best\s+buy|must[\s-]*buy|'
    r'i\s+recommend|our\s+rating|buy\s+alert|'
    r'전문가\s*추천|강력\s*매수|적극\s*매도|'
    # 비금융 콘텐츠 차단 (Axios 등 범용 매체 대비)
    r'recipe|workout|fashion|celebrity|entertainment|'
    r'movie\s+review|tv\s+show|sports\s+score|nfl\s+draft|nba\s+playoff|'
    r'horoscope|weather\s+forecast|travel\s+guide|'
    r'dating|relationship|parenting)',
    re.IGNORECASE
)

# ==========================================
# RSS 피드 설정 (미국 주요 금융 뉴스 사이트)
# 사이트별 주력 추출 데이터 매핑
# ==========================================
RSS_FEEDS = {
    # === Tier 1: 팩트 중심 최우선 소스 (가중치 높음) ===
    "Bloomberg": {
        "url": "https://feeds.bloomberg.com/markets/news.rss",
        "focus": ["장 흐름", "주요 주제"],
        "tier": 1
    },
    "WSJ Markets": {
        "url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
        "focus": ["거시 경제 흐름", "기업 정책"],
        "tier": 1
    },
    "CNBC": {
        "url": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "focus": ["장 흐름", "주요 주제"],
        "tier": 1
    },
    "Financial Times": {
        "url": "https://www.ft.com/?format=rss",
        "focus": ["거시 경제 흐름", "주요 주제"],
        "tier": 1
    },
    "NYT Business": {
        "url": "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
        "focus": ["거시 경제", "기업 정책", "산업 트렌드"],
        "tier": 1
    },
    "Reuters": {
        "url": "https://news.google.com/rss/search?q=site:reuters.com+business+OR+markets+OR+stocks&hl=en-US&gl=US&ceid=US:en",
        "focus": ["속보", "글로벌 시장"],
        "tier": 1
    },
    "AP News": {
        "url": "https://news.google.com/rss/search?q=site:apnews.com+business+OR+economy&hl=en-US&gl=US&ceid=US:en",
        "focus": ["미국 경제정책", "고용"],
        "tier": 1
    },
    "Axios Business": {
        "url": "https://news.google.com/rss/search?q=site:axios.com+business+OR+economy+OR+markets+OR+earnings&hl=en-US&gl=US&ceid=US:en",
        "focus": ["정책", "기업", "팩트 중심 짧은 뉴스"],
        "tier": 1
    },
    "Fed Reserve": {
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
        "focus": ["FOMC 성명", "연준 연설", "금리 결정"],
        "tier": 1
    },
    # === Tier 2: 보조 소스 (속보/데이터 보완) ===
    "MarketWatch": {
        "url": "https://www.marketwatch.com/rss/topstories",
        "focus": ["장중 속보", "변동성 뉴스"],
        "tier": 2
    },
    "Investing.com": {
        "url": "https://www.investing.com/rss/news_25.rss",
        "focus": ["경제 지표 일정", "섹터 뉴스"],
        "tier": 2
    },
    "Yahoo Finance": {
        "url": "https://finance.yahoo.com/rss/topstories",
        "focus": ["인기 종목", "장 흐름"],
        "tier": 2
    },
    "Forbes": {
        "url": "https://www.forbes.com/business/feed/",
        "focus": ["기업 정책", "종목별 이슈"],
        "tier": 2
    },
    "CNBC World": {
        "url": "https://www.cnbc.com/id/100727362/device/rss/rss.html",
        "focus": ["글로벌 시장", "아시아/유럽 동향"],
        "tier": 2
    },
    "CNBC Earnings": {
        "url": "https://www.cnbc.com/id/15839135/device/rss/rss.html",
        "focus": ["실적 발표", "어닝 시즌"],
        "tier": 2
    },
    "Barron's": {
        "url": "https://news.google.com/rss/search?q=site:barrons.com+stocks+OR+markets&hl=en-US&gl=US&ceid=US:en",
        "focus": ["심층 시장 분석", "투자전략"],
        "tier": 2
    },
    "TechCrunch": {
        "url": "https://techcrunch.com/feed/",
        "focus": ["테크 섹터", "스타트업", "IPO"],
        "tier": 2
    },
    "The Verge": {
        "url": "https://www.theverge.com/rss/index.xml",
        "focus": ["테크 산업", "AI", "빅테크"],
        "tier": 2
    },
    "CNBC Tech": {
        "url": "https://www.cnbc.com/id/19854910/device/rss/rss.html",
        "focus": ["기술주", "AI", "반도체"],
        "tier": 2
    },
    "The Economist": {
        "url": "https://www.economist.com/finance-and-economics/rss.xml",
        "focus": ["거시경제 심층분석", "글로벌 트렌드"],
        "tier": 2
    },
    "SEC News": {
        "url": "https://news.google.com/rss/search?q=SEC+filing+OR+SEC+investigation+OR+SEC+ruling+stocks&hl=en-US&gl=US&ceid=US:en",
        "focus": ["SEC 공시", "규제 결정", "조사"],
        "tier": 2
    },
    # === Tier 3: 글로벌 시각 보완 ===
    "BBC Business": {
        "url": "https://feeds.bbci.co.uk/news/business/rss.xml",
        "focus": ["글로벌 경제", "지정학 이슈"],
        "tier": 3
    },
    "AlJazeera Economy": {
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "focus": ["지정학 이슈", "글로벌 경제"],
        "tier": 3
    },
    "NPR Economy": {
        "url": "https://feeds.npr.org/1006/rss.xml",
        "focus": ["미국 경제 정책", "고용/물가"],
        "tier": 3
    },
    "SCMP Economy": {
        "url": "https://www.scmp.com/rss/91/feed",
        "focus": ["중국 경제", "아시아 시장"],
        "tier": 3
    },
    # === 제외: Seeking Alpha (종목 추천/의견 기사 비율 높음) ===
    # === 제외: ForexFactory (RSS 차단됨) ===
}

# 종목 추천/주관적 매체 블랙리스트 (수집 자체를 차단)
BLOCKED_SOURCES = {
    'Seeking Alpha', 'The Motley Fool', 'Zacks', 'Benzinga',
    'InvestorPlace', 'TipRanks', 'Stock Analysis', '24/7 Wall St',
}

# 제목 블랙리스트: 주관적 예측/추천 패턴 (대소문자 무시)
TITLE_BLACKLIST = re.compile(
    r'(top\s+picks?|could\s+rally|could\s+soar|could\s+plunge|could\s+crash|'
    r'analyst\s+says?|analysts?\s+say|target\s+price|price\s+target|'
    r'best\s+stocks?\s+to\s+buy|stocks?\s+to\s+buy\s+now|'
    r'stocks?\s+to\s+watch|stocks?\s+to\s+sell|'
    r'buy\s+the\s+dip|time\s+to\s+buy|time\s+to\s+sell|'
    r'is\s+it\s+time\s+to\s+buy|should\s+you\s+buy|should\s+you\s+sell|'
    r'will\s+it\s+go\s+up|will\s+it\s+go\s+down|'
    r'undervalued|overvalued|hidden\s+gem|under\s+the\s+radar|'
    r'millionaire\s+maker|get\s+rich|passive\s+income|'
    r'dividend\s+aristocrat|dividend\s+king|'
    r'wall\s+street\s+loves|wall\s+street\s+hates|'
    r'my\s+top|our\s+top|my\s+favorite|our\s+favorite|'
    r'\d+\s+best\s+stocks?|\d+\s+worst\s+stocks?|'
    r'\d+\s+reasons?\s+to\s+buy|\d+\s+reasons?\s+to\s+sell|'
    r'magnificent\s+seven\s+stock\s+to\s+buy|'
    r'bargain\s+stock|no[\s-]*brainer\s+stock|'
    r'prediction\s+for\s+\d{4}|forecast\s+for\s+\d{4}|'
    r'where\s+will.*be\s+in\s+\d+\s+year)',
    re.IGNORECASE
)

# Finviz는 스크래핑 권장 (별도 처리)
FINVIZ_URL = "https://finviz.com/news.ashx"

# ==========================================
# 데이터베이스 설정
# ==========================================
DB_PATH = "news_archive.db"

def init_database():
    """데이터베이스 초기화"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 뉴스 아카이브 테이블
    c.execute('''
        CREATE TABLE IF NOT EXISTS news_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash TEXT UNIQUE,
            site TEXT,
            title TEXT,
            summary TEXT,
            link TEXT,
            published_date TEXT,
            collected_date TEXT,
            content_hash TEXT,
            UNIQUE(hash)
        )
    ''')
    
    # 수집 로그 테이블
    c.execute('''
        CREATE TABLE IF NOT EXISTS collection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            site TEXT,
            new_count INTEGER,
            duplicate_count INTEGER,
            status TEXT
        )
    ''')
    
    # 인덱스 추가 (성능 향상)
    try:
        c.execute('CREATE INDEX IF NOT EXISTS idx_hash ON news_archive(hash)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_collected_date ON news_archive(collected_date)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_site ON news_archive(site)')
    except:
        pass
    
    conn.commit()
    conn.close()

# 스레드 로컬 스토리지 (각 스레드별 DB 연결)
thread_local = threading.local()

def get_db_connection():
    """스레드별 DB 연결 가져오기"""
    if not hasattr(thread_local, 'conn'):
        thread_local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        thread_local.conn.execute('PRAGMA journal_mode=WAL')  # Write-Ahead Logging으로 성능 향상
    return thread_local.conn

def generate_hash(title: str, link: str) -> str:
    """뉴스 아이템의 고유 해시 생성 (중복 제거용)"""
    combined = f"{title}|{link}"
    return hashlib.md5(combined.encode()).hexdigest()

def collect_single_feed(site: str, feed_info: Dict, use_sheets: bool = True) -> tuple:
    """단일 RSS 피드 수집 (병렬 처리용)"""
    try:
        url = feed_info["url"] if isinstance(feed_info, dict) else feed_info
        focus_areas = feed_info.get("focus", []) if isinstance(feed_info, dict) else []
        
        # 타임아웃 설정을 위한 requests 사용
        
        try:
            # 10초 타임아웃으로 RSS 피드 가져오기
            response = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            })
            response.raise_for_status()
            feed_content = response.text
            feed = feedparser.parse(feed_content)
        except requests.exceptions.Timeout:
            return (site, [], 0, 0, "TIMEOUT")
        except requests.exceptions.RequestException as e:
            return (site, [], 0, 0, f"NETWORK_ERROR: {str(e)}")
        except Exception as e:
            # fallback to feedparser 직접 호출
            try:
                feed = feedparser.parse(url)
            except:
                return (site, [], 0, 0, f"PARSE_ERROR: {str(e)}")
        
        if feed.bozo and feed.bozo_exception:
            return (site, [], 0, 0, f"ERROR: {feed.bozo_exception}")
        
        if not feed.entries:
            return (site, [], 0, 0, "NO_ENTRIES")
        
        feed_news = []
        new_count = 0
        duplicate_count = 0
        
        # SQLite는 use_sheets=False일 때만 사용
        if not use_sheets:
            conn = get_db_connection()
            c = conn.cursor()
            batch_inserts = []
        
        for entry in feed.entries:
            title = entry.get('title', '').strip()
            link = entry.get('link', '').strip()
            summary = entry.get('summary', entry.get('description', '')).strip()
            
            # HTML 태그 제거
            summary = re.sub(r'<[^>]+>', '', summary)
            
            # published_date 처리 (UTC 기준)
            published_date = ''
            pub_dt = None
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                try:
                    pub_dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                    published_date = pub_dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pub_dt = None

            # published_date 없으면 updated_parsed 시도
            if not pub_dt and hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                try:
                    pub_dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
                    published_date = pub_dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pub_dt = None

            # 날짜 정보 없으면 스킵 (오래된 뉴스 유입 방지)
            if not pub_dt:
                continue

            # 빈 데이터 체크
            if not title or not link:
                continue

            # 12시간 이상 된 기사 필터링 (더 엄격하게)
            now_utc = datetime.now(timezone.utc)
            age_hours = (now_utc - pub_dt).total_seconds() / 3600
            if age_hours > 12:
                continue

            # 광고/칼럼/의견성 콘텐츠 원천 차단 (팩트 기반 뉴스만 수집)
            if NOISE_PATTERNS.search(title) or NOISE_PATTERNS.search(summary[:300]):
                continue

            # 주관적 예측/종목 추천 제목 차단
            if TITLE_BLACKLIST.search(title):
                continue
            
            collected_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # SQLite 중복 체크 (use_sheets=False일 때만)
            if not use_sheets:
                news_hash = generate_hash(title, link)
                c.execute('SELECT id FROM news_archive WHERE hash = ?', (news_hash,))
                if c.fetchone():
                    duplicate_count += 1
                    continue
                
                content_hash = hashlib.md5(summary.encode()).hexdigest()
                batch_inserts.append((
                    news_hash, site, title, summary, link, published_date, collected_date, content_hash
                ))
            
            feed_news.append({
                'site': site,
                'title': title,
                'summary': summary,
                'link': link,
                'published_date': published_date,
                'collected_date': collected_date,
                'focus_areas': focus_areas
            })
            new_count += 1
        
        # SQLite 배치 삽입 (use_sheets=False일 때만)
        if not use_sheets and batch_inserts:
            c.executemany('''
                INSERT INTO news_archive 
                (hash, site, title, summary, link, published_date, collected_date, content_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', batch_inserts)
            conn.commit()
        
        return (site, feed_news, new_count, duplicate_count, "SUCCESS")
        
    except Exception as e:
        return (site, [], 0, 0, f"ERROR: {str(e)}")

def collect_rss_feeds(use_sheets: bool = True) -> List[Dict]:
    """모든 RSS 피드에서 뉴스 수집 (병렬 처리, 구글 시트 옵션)"""
    if not use_sheets:
        init_database()
    
    all_news = []
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"\n{'='*60}")
    print(f"📰 RSS 피드 수집 시작 ({timestamp}) - 병렬 처리")
    if use_sheets:
        print("📊 구글 시트 아카이브 사용")
    print('='*60)
    
    # 병렬 처리로 모든 피드 동시 수집 (블랙리스트 소스 제외)
    active_feeds = {s: f for s, f in RSS_FEEDS.items() if s not in BLOCKED_SOURCES}
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = {
            executor.submit(collect_single_feed, site, feed_info, use_sheets): site
            for site, feed_info in active_feeds.items()
        }
        
        try:
            for future in as_completed(futures, timeout=120):  # 전체 타임아웃 2분
                site = futures[future]
                try:
                    site_name, feed_news, new_count, duplicate_count, status = future.result(timeout=15)  # 개별 피드 타임아웃 15초
                    
                    print(f"✅ {site_name}: {new_count}개 신규, {duplicate_count}개 중복 ({status})")
                    
                    all_news.extend(feed_news)
                    
                    # 로그 기록 (메인 스레드에서)
                    if not use_sheets:
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute('''
                            INSERT INTO collection_log (timestamp, site, new_count, duplicate_count, status)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (timestamp, site_name, new_count, duplicate_count, status))
                        conn.commit()
                        conn.close()
                    
                except FutureTimeoutError:
                    print(f"⚠️ {site} 타임아웃 (15초 초과)")
                except Exception as e:
                    print(f"⚠️ {site} 처리 중 오류: {e}")
                    import traceback
                    traceback.print_exc()
        except FutureTimeoutError:
            print(f"⚠️ 전체 RSS 수집 타임아웃 (2분 초과), 수집된 {len(all_news)}개 뉴스 저장 진행")
    
    print(f"\n✅ 총 {len(all_news)}개 뉴스 수집 완료")
    
    # 구글 시트에 저장 (옵션)
    if use_sheets and all_news:
        try:
            from google_sheets_archive import get_sheets_archive, GSPREAD_AVAILABLE
            if GSPREAD_AVAILABLE:
                from gemini_summarizer import summarize_news
                from data_cleaner import clean_news_items
                
                print("📊 구글 시트에 저장 중...")
                sheets = get_sheets_archive()
                
                # 데이터 정리 (노이즈 필터링, 중복 제거)
                print("🧹 데이터 정리 중...")
                cleaned_news = clean_news_items(all_news)
                print(f"✅ {len(cleaned_news)}개 뉴스 정리 완료 (제거: {len(all_news) - len(cleaned_news)}개)")
                
                # Gemini로 요약 생성 (카테고리 태그, 중요도 점수 포함)
                summaries = summarize_news(cleaned_news, batch_size=5)
                
                # 구글 시트에 추가 (날짜별 탭에 자동 저장)
                sheets.add_news(cleaned_news, summaries)
                
                print("✅ 구글 시트 저장 완료")
            else:
                print("⚠️ gspread 미설치, 구글 시트 저장 건너뜀")
        except Exception as e:
            print(f"⚠️ 구글 시트 저장 실패: {e}")
            import traceback
            traceback.print_exc()
            # 실패해도 뉴스는 반환
    
    return all_news

def get_recent_news(limit: int = 200, days: int = 3, use_sheets: bool = True) -> List[Dict]:
    """최근 뉴스 가져오기 (구글 시트 우선)"""
    # 구글 시트에서 가져오기 (기본값)
    if use_sheets:
        try:
            from google_sheets_archive import get_sheets_archive, GSPREAD_AVAILABLE
            if GSPREAD_AVAILABLE:
                sheets = get_sheets_archive()
                news_items = sheets.get_recent_news(limit=limit, days=days)
                if news_items:
                    print(f"✅ 구글 시트에서 {len(news_items)}개 뉴스 로드")
                    return news_items
            else:
                print("⚠️ gspread 미설치, SQLite 사용")
        except Exception as e:
            print(f"⚠️ 구글 시트 로드 실패: {e}, SQLite 사용")
    
    # SQLite 폴백 (T와 T-1, KST 기준)
    init_database()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    try:
        from zoneinfo import ZoneInfo
        kst_now = datetime.now(ZoneInfo('Asia/Seoul'))
    except ImportError:
        kst_now = datetime.now(timezone(timedelta(hours=9)))
    cutoff_date = (kst_now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')

    c.execute('''
        SELECT site, title, summary, link, published_date, collected_date
        FROM news_archive
        WHERE collected_date >= ?
        ORDER BY collected_date DESC
        LIMIT ?
    ''', (cutoff_str, limit))
    
    rows = c.fetchall()
    conn.close()
    
    news_items = []
    for row in rows:
        news_items.append({
            'site': row[0],
            'title': row[1],
            'summary': row[2],
            'link': row[3],
            'published_date': row[4],
            'collected_date': row[5]
        })
    
    return news_items

def get_today_news() -> List[Dict]:
    """오늘 수집된 뉴스 가져오기"""
    return get_recent_news(limit=200, days=1)

def deduplicate_news(news_items: List[Dict]) -> List[Dict]:
    """중복 뉴스 제거 (제목 유사도 기반)"""
    if not news_items:
        return []
    
    # 해시 기반 중복 제거
    seen_hashes = set()
    unique_news = []
    
    for news in news_items:
        news_hash = generate_hash(news['title'], news['link'])
        if news_hash not in seen_hashes:
            seen_hashes.add(news_hash)
            unique_news.append(news)
    
    return unique_news

if __name__ == "__main__":
    # 테스트 실행
    print("🚀 Global Alpha Reader - RSS 수집 테스트")
    news = collect_rss_feeds()
    print(f"\n✅ 수집된 뉴스: {len(news)}개")
    
    if news:
        print("\n📰 최신 뉴스 샘플:")
        for i, item in enumerate(news[:5], 1):
            print(f"\n{i}. [{item['site']}] {item['title']}")
            print(f"   {item['link']}")
