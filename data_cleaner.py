#!/usr/bin/env python3
"""
데이터 정리 모듈
중복 제거, 필터링, 유사도 체크 등
"""

from typing import List, Dict
import re
from difflib import SequenceMatcher

# 키워드 블랙리스트 (제목에 포함되면 제외) - 강화된 필터링
KEYWORD_BLACKLIST = [
    # ==========================================
    # 의견/칼럼/편집 관련
    # ==========================================
    'opinion', 'column', 'editorial', 'commentary', 'analysis:', 
    'view:', 'perspective', 'opinion:', 'column:',
    
    # ==========================================
    # 광고 관련
    # ==========================================
    'sponsored', 'paid', 'advertisement', 'ad', 'advert', 'ad:',
    'promoted', 'sponsor', 'advertorial', 'affiliate', 'affiliate link',
    'buy now', 'shop now', 'shop', 'click here', 'learn more',
    
    # ==========================================
    # 홍보/마케팅 관련
    # ==========================================
    'promotion', 'promo', 'discount', 'coupon', 'sale',
    'limited time', 'act now', 'special offer', 'deal of the day',
    'free shipping', 'best price',
    
    # ==========================================
    # 구독/뉴스레터 관련
    # ==========================================
    'newsletter', 'subscribe', 'sign up', 'sign up for', 
    'join our', 'email list', 'mailing list', 'opt-in',
    
    # ==========================================
    # 소셜 미디어 관련
    # ==========================================
    'follow us', 'like us', 'share', 'share this', 'tweet this',
    'instagram', 'facebook page', 'social media',
    
    # ==========================================
    # 이벤트/웨비나 관련
    # ==========================================
    'webinar', 'event', 'register now', 'conference', 'summit', 
    'workshop', 'book now', 'reserve your spot',
    
    # ==========================================
    # 게임/퍼즐 키워드 (강화)
    # ==========================================
    'puzzle', 'game', 'pips', 'domino', 'crossword', 'sudoku',
    
    # ==========================================
    # 가이드/공략 키워드 (강화)
    # ==========================================
    'guide', 'walkthrough', 'solution', 'answer', 'hint',
    'how to solve', 'how to play', 'game guide', 'puzzle guide',
    'tips', 'trick', 'cheat',
    
    # ==========================================
    # 엔터테인먼트 필터링 (강화)
    # ==========================================
    'entertainment', 'sports', 'celebrity', 'gossip',
    'football', 'basketball', 'baseball', 'soccer',
    'entertainment news', 'movie', 'tv show', 'lifestyle',
    
    # ==========================================
    # 비금융 콘텐츠 필터링 (강화)
    # ==========================================
    'recipe', 'cooking', 'food', 'travel', 'vacation', 'hotel', 
    'restaurant', 'review', 'health tips', 'wellness', 'fitness', 
    'diet', 'exercise', 'tutorial', 'how to', 'learn', 'course', 
    'class', 'lesson', 'education', 'school', 'university',
    'home decor', 'interior design', 'gardening', 'diy',
    'weather', 'forecast', 'climate',
    
    # ==========================================
    # 기타 광고성 표현
    # ==========================================
    'exclusive', 'breaking:', 'alert:', 'warning:', 'urgent:',
    'must read', 'you won\'t believe', 'shocking', 'amazing',
    'incredible', 'unbelievable', 'secret', 'hidden', 'revealed',
    'clickbait', 'viral', 'trending now', 'hot deal',
    'product review', 'service review', 'best of', 'top 10',
    'comparison', 'vs.', 'versus', 'which is better',
    'donate', 'support us', 'contribute', 'fundraising',
    'crowdfunding', 'kickstarter', 'indiegogo',
    'medical advice', 'doctor says',
    'real estate tips', 'house hunting',
    'science news (non-financial)',
    'technology review', 'gadget review', 'product launch (non-stock)'
]

def calculate_similarity(str1: str, str2: str) -> float:
    """
    두 문자열의 유사도 계산 (0.0 ~ 1.0)
    SequenceMatcher 사용
    """
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()

def is_title_similar(title1: str, title2: str, threshold: float = 0.9) -> bool:
    """
    두 제목의 유사도가 threshold 이상인지 확인
    기본값: 90% 이상
    """
    similarity = calculate_similarity(title1, title2)
    return similarity >= threshold

def filter_noise(title: str, summary: str = '') -> bool:
    """
    노이즈 필터링 (강화)
    제목 또는 요약에 블랙리스트 키워드가 포함되어 있으면 True 반환 (제외해야 함)
    """
    title_lower = title.lower()
    summary_lower = summary.lower() if summary else ''
    combined_text = f"{title_lower} {summary_lower}"
    
    # 제목에서 직접 체크
    for keyword in KEYWORD_BLACKLIST:
        if keyword in title_lower:
            return True  # 제외해야 함
    
    # 요약에서도 체크 (광고성 내용 + 비금융 콘텐츠) - 강화
    summary_keywords = [
        # 광고/홍보
        'sponsored', 'advertisement', 'advertorial', 'affiliate', 
        'buy now', 'shop now', 'click here', 'sign up', 'subscribe',
        'promotion', 'discount', 'sale', 'coupon',
        'follow us', 'like us', 'share',
        'webinar', 'event', 'register now',
        # 게임/퍼즐
        'puzzle', 'game', 'pips', 'domino', 'crossword', 'sudoku',
        'game guide', 'walkthrough', 'solution', 'answer', 'hint',
        'how to solve', 'how to play',
        # 엔터테인먼트
        'sports', 'celebrity', 'gossip', 'entertainment',
        # 비금융 콘텐츠
        'recipe', 'cooking', 'food', 'travel', 'vacation',
        'health tips', 'fitness', 'diet', 'exercise',
        'how to', 'tutorial', 'learn', 'course',
        # 의견/칼럼
        'opinion', 'column', 'editorial', 'commentary'
    ]
    for keyword in summary_keywords:
        if keyword in summary_lower:
            return True
    
    # 링크 패턴 체크 (광고성 도메인)
    ad_domains = ['amazon.com', 'ebay.com', 'etsy.com', 'shopify', 
                  'affiliate', 'partner', 'sponsor']
    for domain in ad_domains:
        if domain in combined_text:
            return True
    
    # 너무 짧은 제목 (광고 가능성)
    if len(title.strip()) < 10:
        return True
    
    # 게임/퍼즐/가이드 관련 키워드 체크 (제목 + 요약) - 강화
    game_keywords = [
        'puzzle', 'game', 'pips', 'domino', 'crossword', 'sudoku',
        'guide', 'walkthrough', 'solution', 'answer', 'hint',
        'how to solve', 'how to play', 'game guide', 'puzzle guide'
    ]
    for keyword in game_keywords:
        if keyword in combined_text:
            return True
    
    # NYT 퍼즐/게임 콘텐츠 특별 체크 (강화)
    nyt_keywords = ['nyt', 'new york times']
    game_puzzle_keywords = ['puzzle', 'game', 'pips', 'domino', 'crossword', 'sudoku', 
                           'guide', 'walkthrough', 'solution', 'answer', 'hint']
    
    has_nyt = any(kw in combined_text for kw in nyt_keywords)
    has_game_puzzle = any(kw in combined_text for kw in game_puzzle_keywords)
    
    if has_nyt and has_game_puzzle:
        return True  # NYT + 게임/퍼즐 키워드 조합 감지
    
    # 대문자 과다 사용 (광고성 표현)
    if len(title) > 0:
        upper_ratio = sum(1 for c in title if c.isupper()) / len(title)
        if upper_ratio > 0.5 and len(title) > 20:  # 50% 이상 대문자
            return True
    
    return False  # 포함 가능

def check_duplicate_by_title(new_title: str, existing_titles: List[str], threshold: float = 0.9) -> bool:
    """
    새 제목이 기존 제목들과 유사한지 확인
    threshold 이상 유사하면 중복으로 판단
    """
    for existing_title in existing_titles:
        if is_title_similar(new_title, existing_title, threshold):
            return True  # 중복
    return False  # 중복 아님

def clean_news_items(news_items: List[Dict], existing_titles: List[str] = None) -> List[Dict]:
    """
    뉴스 아이템 정리 (강화된 필터링)
    - 노이즈 필터링 (제목 + 요약 체크)
    - 제목 기반 중복 제거
    """
    if existing_titles is None:
        existing_titles = []
    
    cleaned_items = []
    filtered_count = 0
    duplicate_count = 0
    
    for news in news_items:
        title = news.get('title', '').strip()
        summary = news.get('summary', '').strip()
        
        if not title:
            continue
        
        # 1. 노이즈 필터링 (제목 + 요약 모두 체크)
        if filter_noise(title, summary):
            filtered_count += 1
            continue
        
        # 2. 제목 기반 중복 제거 (유사도 90% 이상)
        if check_duplicate_by_title(title, existing_titles, threshold=0.9):
            duplicate_count += 1
            continue
        
        # 통과한 뉴스 추가
        cleaned_items.append(news)
        existing_titles.append(title)
    
    if filtered_count > 0 or duplicate_count > 0:
        print(f"🧹 데이터 정리: {filtered_count}개 노이즈 제거, {duplicate_count}개 중복 제거")
    
    return cleaned_items
