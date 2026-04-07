#!/usr/bin/env python3
"""
번역 모듈 - 영어 뉴스를 한글로 번역 (Gemini 배치 번역)
"""

import json
import time
import google.generativeai as genai

import os
from dotenv import load_dotenv
load_dotenv()
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

def _has_korean(text):
    return text and any('\uAC00' <= c <= '\uD7A3' for c in text)

def translate_to_korean(text: str) -> str:
    """단일 텍스트 한국어 번역"""
    if not text or _has_korean(text):
        return text
    try:
        prompt = f"다음 영어 뉴스 제목을 자연스러운 한국어로 번역해주세요. 금융/주식 용어는 정확하게 번역하세요. 번역 결과만 출력하세요.\n\n{text}"
        response = model.generate_content(prompt)
        result = response.text.strip() if hasattr(response, 'text') else ''
        return result if result and _has_korean(result) else text
    except Exception as e:
        print(f"⚠️ 번역 실패: {e}")
        return text


def _do_batch_translate(batch: list) -> dict:
    """배치 번역 1회 시도. {seq_key: translated_text} 반환"""
    seq_to_idx = {str(seq): idx for seq, (idx, _) in enumerate(batch)}
    titles_json = json.dumps(
        {str(seq): title for seq, (_, title) in enumerate(batch)},
        ensure_ascii=False
    )

    prompt = f"""다음 영어 뉴스 제목들을 자연스러운 한국어로 번역해주세요.
금융/주식 용어는 정확하게 번역하세요.
반드시 아래 JSON 형식으로만 응답하세요. 다른 텍스트는 포함하지 마세요.

입력:
{titles_json}

출력 형식 (JSON만):
{{"0": "번역된 제목", "1": "번역된 제목", ...}}"""

    response = model.generate_content(prompt)
    response_text = response.text.strip() if hasattr(response, 'text') else ''

    # JSON 코드블록 제거
    if '```' in response_text:
        parts = response_text.split('```')
        for part in parts:
            part = part.strip()
            if part.startswith('json'):
                part = part[4:].strip()
            if part.startswith('{'):
                response_text = part
                break

    return json.loads(response_text), seq_to_idx


def translate_news_batch(news_items: list, max_items: int = 50) -> list:
    """배치 뉴스 제목 한국어 번역"""
    if not news_items:
        return news_items

    items_to_translate = news_items[:max_items]

    # 번역 필요한 항목만 수집
    needs_translation = []
    for i, news in enumerate(items_to_translate):
        title = news.get('title', '')
        title_kr = news.get('title_kr', '')
        if _has_korean(title_kr):
            continue
        if title and not _has_korean(title):
            needs_translation.append((i, title))

    if not needs_translation:
        return items_to_translate

    BATCH_SIZE = 5  # 배치 크기 줄여서 Gemini 실패율 감소
    for batch_start in range(0, len(needs_translation), BATCH_SIZE):
        batch = needs_translation[batch_start:batch_start + BATCH_SIZE]

        success = False
        # 1차 시도: 배치 번역
        try:
            translations, seq_to_idx = _do_batch_translate(batch)
            for seq_key, orig_idx in seq_to_idx.items():
                translated = translations.get(seq_key, '')
                original_title = batch[int(seq_key)][1]
                if translated and _has_korean(translated) and len(translated) <= len(original_title) * 4:
                    items_to_translate[orig_idx]['title_kr'] = translated
                else:
                    items_to_translate[orig_idx]['title_kr'] = original_title
            success = True
        except Exception as e:
            print(f"⚠️ 배치 번역 1차 실패: {e}")

        # 2차 시도: 실패한 항목만 재시도 (더 작은 배치)
        if not success:
            time.sleep(0.5)
            failed_items = [
                (idx, title) for idx, title in batch
                if not _has_korean(items_to_translate[idx].get('title_kr', ''))
            ]
            if failed_items:
                # 2-3개씩 재시도
                for sub_start in range(0, len(failed_items), 3):
                    sub_batch = failed_items[sub_start:sub_start + 3]
                    try:
                        translations2, seq_to_idx2 = _do_batch_translate(sub_batch)
                        for seq_key, orig_idx in seq_to_idx2.items():
                            translated = translations2.get(seq_key, '')
                            original_title = sub_batch[int(seq_key)][1]
                            if translated and _has_korean(translated):
                                items_to_translate[orig_idx]['title_kr'] = translated
                            else:
                                # 개별 번역으로 최후 시도
                                items_to_translate[orig_idx]['title_kr'] = translate_to_korean(original_title)
                    except Exception as e2:
                        print(f"⚠️ 배치 번역 2차 실패: {e2}")
                        for idx, title in sub_batch:
                            items_to_translate[idx]['title_kr'] = translate_to_korean(title)

    # summary_kr 설정
    for news in items_to_translate:
        if not news.get('summary_kr'):
            news['summary_kr'] = news.get('title_kr', news.get('summary', ''))

    return items_to_translate
