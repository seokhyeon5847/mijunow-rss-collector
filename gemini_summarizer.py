#!/usr/bin/env python3
"""
Gemini를 사용한 뉴스 요약 모듈
팩트 중심의 요약문 생성 + buzz_score(화제성) 추가
"""

import google.generativeai as genai
from typing import List, Dict
import time
import json
import re

# Gemini API 설정
import os
from dotenv import load_dotenv
load_dotenv()
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash-lite')

def _call_gemini_with_retry(prompt, max_retries=3):
    """Gemini API 호출 (지수 백오프 재시도)"""
    for attempt in range(max_retries):
        try:
            response = model.generate_content(prompt)
            return response
        except Exception as e:
            print(f"⚠️ Gemini 호출 실패 ({attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait = (2 ** attempt) + 1
                print(f"   {wait}초 후 재시도...")
                time.sleep(wait)
    class _FakeResponse:
        text = '[]'
    return _FakeResponse()


def _build_batch_prompt(batch_text: str) -> str:
    """배치 요약 프롬프트 생성 (buzz_score 포함)"""
    return f"""당신은 글로벌 금융 뉴스 큐레이터입니다.
미국 주식/경제 인스타그램 채널 "미주나우"의 뉴스 데이터베이스를 관리합니다.

## 당신의 역할
RSS 피드에서 수집된 뉴스 기사들을 분석하여, 각 기사에 대해 아래 정보를 생성합니다.

## 출력 형식 (각 기사마다)
1. summary_kr: 한국어 요약 (2-3문장, 팩트 중심)
2. category: 카테고리 (아래 13종 중 택1)
3. tickers: 관련 종목 티커 (없으면 빈 문자열)
4. importance: 뉴스 중요도 (1-10)
5. buzz_score: 실시간 화제성 점수 (1-10)
6. fact_score: 팩트 점수 (1-10)

## 카테고리 (13종)
Macro, Policy, Tech, Energy, Finance, Earnings, Commodities, Biotech, RealEstate,
Crypto, Retail, SEC, Other

## 중요도(importance) 채점 기준

10점: 시장 전체를 움직이는 이벤트
  - 연준 금리 결정, 대규모 지정학 사건, GDP/고용 서프라이즈
  - 예: "연준 긴급 금리 인하", "이란 에너지 시설 공격"

9점: 특정 섹터/대형주에 큰 영향
  - 빅테크 실적 서프라이즈, 대규모 M&A, 주요 규제 변경
  - 예: "엔비디아 실적 3배 증가", "우버-리비안 12.5억 달러 투자"

8점: 시장 참여자들이 주목하는 이벤트
  - 주요 경제지표 발표, 섹터 트렌드 변화, 중요 인사 발언
  - 예: "파월 의장 인플레이션 경고", "유가 100달러 돌파"

7점: 투자 판단에 참고할 만한 뉴스
  - 개별 기업 공시, 업종 동향, 정책 변화 조짐

5-6점: 일반적인 시장 뉴스 (배경 정보)
3-4점: 관련성 낮은 뉴스
1-2점: 금융/투자와 무관한 뉴스

## 실시간 화제성(buzz_score) 채점 기준

이 점수는 "이 기사가 지금 시장에서 얼마나 화제인가"를 측정합니다.

채점 시 반드시 고려할 것:
1. 이번 배치 내에서 같은 주제/이슈를 다루는 기사가 여러 개인가?
   → 같은 이슈가 2개 매체: buzz 7, 3개 매체: buzz 8, 4개+: buzz 9-10
2. 이 뉴스가 "방금 터진" 속보인가, 아니면 진행 중인 이슈의 반복 보도인가?
   → 속보(첫 보도): buzz +2, 후속 보도: buzz +0
3. 소셜미디어/투자자 커뮤니티에서 화제가 될 만한 뉴스인가?
   → 충격적 수치, 유명 인물, 논쟁적 주제: buzz +1

10점: 전 매체가 동시 보도 중인 초대형 속보
8-9점: 다수 매체가 보도 중인 핫 이슈 (3-4개 매체)
6-7점: 2개 매체 이상 보도 또는 투자자 관심 높은 주제
4-5점: 단일 매체 보도, 일반적 관심도
1-3점: 관심도 낮은 단독 보도

## 중복 뉴스 처리 (매우 중요)

같은 이슈를 여러 매체에서 보도한 경우:
- 각 기사를 개별적으로 요약하되, 모든 기사의 buzz_score를 동일하게 높게 매기세요.
- 중복 보도 자체가 "이 이슈가 지금 뜨겁다"는 강력한 신호입니다.
- 제목이 다르더라도 같은 사건/이슈를 다루면 중복으로 판단하세요.

## 팩트 점수(fact_score) 채점 기준
- 10점: 공시, 실적 발표, 정부 공식 발표, 경제 지표
- 8-9점: 인수합병 확정, 기업 공식 발표, 법원 판결
- 6-7점: 기자 취재 기반 (출처 명시, 수치 포함)
- 4-5점: 업계 관계자 인용, 루머 기반
- 1-3점: 칼럼니스트 의견, 애널리스트 예측, 종목 추천

## 한국어 요약 작성 규칙
- 2-3문장, 팩트 중심
- 구체적 수치 포함 (금액, 등락률, 날짜 등)
- 의견/추측이 아닌 확인된 사실만
- 한국 투자자 관점에서 왜 중요한지 맥락 포함

## 티커 추출 규칙
- 미국 상장 종목만 (NYSE, NASDAQ)
- 직접 언급되거나 명백히 영향받는 종목
- 복수 종목은 쉼표로 구분
- 지수(S&P 500, 나스닥 등)는 티커로 추출하지 마세요

## 즉시 제외 대상 (해당 기사는 JSON에서 제외)
- 광고, 홍보, 뉴스레터 구독 유도
- 종목 추천 ("Top Picks", "Best Stocks to Buy" 등)
- 주관적 주가 예측 ("Could Rally", "Will Soar" 등)
- 비금융 콘텐츠 (게임, 스포츠, 요리, 여행 등)

---
{batch_text}
---

위 뉴스들을 분석하여 **반드시 유효한 JSON 배열만** 출력하세요. 다른 텍스트 없이 JSON만:
[
  {{
    "index": 1,
    "summary_kr": "한국어 요약 2-3문장",
    "category": "카테고리",
    "tickers": "AAPL, MSFT",
    "importance": 9,
    "buzz_score": 8,
    "fact_score": 9
  }}
]

광고/의견/비금융 기사는 JSON에서 완전히 제외하세요."""


def _parse_json_response(result_text: str, batch_links: list, batch: list) -> Dict[str, Dict]:
    """JSON 응답 파싱 (폴백 포함)"""
    summaries = {}

    # JSON 추출 (```json ... ``` 또는 [ ... ] 형태)
    json_match = re.search(r'\[[\s\S]*\]', result_text)
    if not json_match:
        print("⚠️ JSON 파싱 실패 — 폴백 사용")
        return _fallback_parse(result_text, batch_links, batch)

    try:
        items = json.loads(json_match.group())
    except json.JSONDecodeError:
        print("⚠️ JSON 디코드 실패 — 폴백 사용")
        return _fallback_parse(result_text, batch_links, batch)

    for item in items:
        idx = item.get('index', 0) - 1  # 1-based → 0-based
        if 0 <= idx < len(batch_links):
            link = batch_links[idx]
            summaries[link] = {
                'summary': item.get('summary_kr', ''),
                'category': item.get('category', 'Other'),
                'tickers': item.get('tickers', ''),
                'priority': str(item.get('importance', 5)),
                'buzz_score': str(item.get('buzz_score', 5)),
                'fact_score': str(item.get('fact_score', 5))
            }

    return summaries


def _fallback_parse(result_text: str, batch_links: list, batch: list) -> Dict[str, Dict]:
    """기존 정규식 기반 파싱 (JSON 실패 시 폴백)"""
    summaries = {}
    lines = result_text.split('\n')
    current_index = 0
    current_summary = []
    current_category = ''
    current_tickers = ''
    current_priority = ''
    current_buzz_score = ''
    current_fact_score = ''

    for line in lines:
        line_lower = line.lower().strip()

        if '광고' in line_lower or 'advertisement' in line_lower:
            if current_index < len(batch_links):
                current_index += 1
            continue

        if f'[뉴스 {current_index + 1}]' in line or f'뉴스 {current_index + 1}' in line:
            if current_index > 0 and current_index <= len(batch_links):
                link = batch_links[current_index - 1]
                summaries[link] = {
                    'summary': ' '.join(current_summary) if current_summary else batch[current_index - 1].get('summary', '')[:200],
                    'category': current_category or 'Other',
                    'tickers': current_tickers or '',
                    'priority': current_priority or '5',
                    'buzz_score': current_buzz_score or '5',
                    'fact_score': current_fact_score or '5'
                }
            current_index += 1
            current_summary = []
            current_category = ''
            current_tickers = ''
            current_priority = ''
            current_buzz_score = ''
            current_fact_score = ''
            continue

        if '요약:' in line_lower or 'summary' in line_lower:
            text = line.split(':', 1)[-1].strip()
            if text:
                current_summary.append(text)
        elif '카테고리:' in line_lower or 'category:' in line_lower:
            current_category = line.split(':', 1)[-1].strip()
        elif '티커:' in line_lower or 'ticker' in line_lower:
            t = line.split(':', 1)[-1].strip()
            current_tickers = t if t.lower() not in ['빈 값', 'none', 'n/a', ''] else ''
        elif '중요도:' in line_lower or 'importance:' in line_lower:
            nums = re.findall(r'\d+', line.split(':', 1)[-1])
            if nums:
                current_priority = nums[0]
        elif 'buzz' in line_lower:
            nums = re.findall(r'\d+', line.split(':', 1)[-1])
            if nums:
                current_buzz_score = nums[0]
        elif '팩트:' in line_lower or 'fact' in line_lower:
            nums = re.findall(r'\d+', line.split(':', 1)[-1])
            if nums:
                current_fact_score = nums[0]
        elif current_index > 0 and line.strip() and not line.strip().startswith('['):
            current_summary.append(line.strip())

    # 마지막 뉴스 저장
    if current_index > 0 and current_index <= len(batch_links):
        link = batch_links[current_index - 1]
        summaries[link] = {
            'summary': ' '.join(current_summary) if current_summary else batch[current_index - 1].get('summary', '')[:200],
            'category': current_category or 'Other',
            'tickers': current_tickers or '',
            'priority': current_priority or '5',
            'buzz_score': current_buzz_score or '5',
            'fact_score': current_fact_score or '5'
        }

    return summaries


def summarize_news(news_items: List[Dict], batch_size: int = 15) -> Dict[str, Dict]:
    """
    뉴스 요약 (Gemini 사용, JSON 출력, buzz_score 포함)

    Returns:
        {link: {'summary': str, 'category': str, 'tickers': str,
                'priority': str, 'buzz_score': str, 'fact_score': str}}
    """
    summaries = {}

    if not news_items:
        return summaries

    print(f"🤖 Gemini로 {len(news_items)}개 뉴스 요약 시작 (buzz_score 포함)...")

    for i in range(0, len(news_items), batch_size):
        batch = news_items[i:i+batch_size]

        try:
            batch_text = ""
            batch_links = []

            for news in batch:
                title = news.get('title', '')
                summary = news.get('summary', '')
                link = news.get('link', '')

                batch_text += f"\n[뉴스 {len(batch_links) + 1}]\n"
                batch_text += f"제목: {title}\n"
                batch_text += f"내용: {summary[:500]}\n"
                batch_links.append(link)

            prompt = _build_batch_prompt(batch_text)

            response = _call_gemini_with_retry(prompt)
            result_text = response.text if hasattr(response, 'text') else str(response)

            # JSON 파싱 시도 → 실패 시 정규식 폴백
            batch_summaries = _parse_json_response(result_text, batch_links, batch)
            summaries.update(batch_summaries)

            # 파싱 안 된 뉴스는 기본값
            for j, link in enumerate(batch_links):
                if link not in summaries:
                    summaries[link] = {
                        'summary': batch[j].get('summary', '')[:200],
                        'category': 'Other',
                        'tickers': '',
                        'priority': '5',
                        'buzz_score': '5',
                        'fact_score': '5'
                    }

            print(f"✅ {len(batch_links)}개 뉴스 요약 완료 ({i+len(batch)}/{len(news_items)})")
            time.sleep(0.5)

        except Exception as e:
            print(f"⚠️ 배치 요약 오류: {e}")
            for news in batch:
                link = news.get('link', '')
                if link:
                    summaries[link] = {
                        'summary': news.get('summary', '')[:200],
                        'category': 'Other',
                        'tickers': '',
                        'priority': '5',
                        'buzz_score': '5',
                        'fact_score': '5'
                    }

    print(f"✅ 총 {len(summaries)}개 뉴스 요약 완료 (buzz_score 포함)")
    return summaries


def summarize_single_news(title: str, summary: str) -> str:
    """단일 뉴스 요약"""
    try:
        prompt = f"""다음 뉴스를 팩트 중심으로 2-3문장으로 한국어로 요약해주세요.
구체적 수치를 포함하고, 의견/추측이 아닌 확인된 사실만 추출하세요.

제목: {title}
내용: {summary[:500]}

요약:"""

        response = _call_gemini_with_retry(prompt)
        result = response.text if hasattr(response, 'text') else str(response)
        return result.strip()
    except Exception as e:
        print(f"⚠️ 단일 뉴스 요약 오류: {e}")
        return summary[:200]
