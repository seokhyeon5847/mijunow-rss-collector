# MijuNow RSS Collector

미주나우 - US 주식 뉴스 자동 수집기

GitHub Actions로 15분마다 28개 RSS 피드에서 뉴스를 수집하고 Google Sheets에 저장합니다.

## 수집 소스
- **Tier 1**: Bloomberg, WSJ, CNBC, FT, NYT, Reuters, AP, Axios, Federal Reserve
- **Tier 2**: MarketWatch, Investing.com, Yahoo Finance, Forbes, TechCrunch, Economist, SEC
- **Tier 3**: BBC, Al Jazeera, NPR, SCMP

## 기능
- 28개 RSS 피드 병렬 수집
- Gemini AI 뉴스 요약 + 카테고리 태깅
- 광고/의견 기사 자동 필터링
- Google Sheets 자동 저장
- Hash 기반 중복 제거
