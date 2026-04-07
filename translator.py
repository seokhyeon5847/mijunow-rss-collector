#!/usr/bin/env python3
"""
번역 모듈 - 영어 뉴스를 한글로 번역 (Google Translate 무료 API)
Gemini API 호출 없이 무료로 번역
"""

import json
import time
import urllib.request
import urllib.parse


def _has_korean(text):
    return text and any('\uAC00' <= c <= '\uD7A3' for c in text)


def translate_to_korean(text: str) -> str:
    """단일 텍스트 한국어 번역 (Google Translate 무료)"""
    if not text or _has_korean(text):
        return text
    try:
        encoded = urllib.parse.quote(text)
        url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=en&tl=ko&dt=t&q={encoded}"
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            translated = ''.join(part[0] for part in data[0] if part[0])
            return translated if translated and _has_korean(translated) else text
    except Exception as e:
        print(f"⚠️ 번역 실패: {e}")
        return text


def translate_news_batch(news_items: list, max_items: int = 50) -> list:
    """배치 뉴스 제목 한국어 번역"""
    if not news_items:
        return news_items

    items_to_translate = news_items[:max_items]

    for i, news in enumerate(items_to_translate):
        title = news.get('title', '')
        title_kr = news.get('title_kr', '')

        if _has_korean(title_kr):
            continue

        if title and not _has_korean(title):
            translated = translate_to_korean(title)
            items_to_translate[i]['title_kr'] = translated

            # rate limit 방지 (0.2초 간격)
            if i < len(items_to_translate) - 1:
                time.sleep(0.2)

    # summary_kr 설정
    for news in items_to_translate:
        if not news.get('summary_kr'):
            news['summary_kr'] = news.get('title_kr', news.get('summary', ''))

    return items_to_translate
