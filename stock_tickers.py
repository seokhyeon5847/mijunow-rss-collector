#!/usr/bin/env python3
"""
주요 주식 티커 데이터베이스
실제 거래되는 주요 주식 티커 목록
"""

from typing import List

# 주요 주식 티커 목록 (S&P 500, 나스닥 주요 종목)
MAJOR_STOCK_TICKERS = {
    # Technology
    'AAPL': 'Apple Inc.',
    'MSFT': 'Microsoft Corporation',
    'GOOGL': 'Alphabet Inc.',
    'GOOG': 'Alphabet Inc.',
    'AMZN': 'Amazon.com Inc.',
    'NVDA': 'NVIDIA Corporation',
    'META': 'Meta Platforms Inc.',
    'TSLA': 'Tesla Inc.',
    'NFLX': 'Netflix Inc.',
    'AMD': 'Advanced Micro Devices',
    'INTC': 'Intel Corporation',
    'CRM': 'Salesforce.com Inc.',
    'ORCL': 'Oracle Corporation',
    'ADBE': 'Adobe Inc.',
    'CSCO': 'Cisco Systems Inc.',
    
    # Finance
    'JPM': 'JPMorgan Chase & Co.',
    'BAC': 'Bank of America Corp',
    'WFC': 'Wells Fargo & Company',
    'GS': 'Goldman Sachs Group Inc.',
    'MS': 'Morgan Stanley',
    'C': 'Citigroup Inc.',
    'AXP': 'American Express Company',
    
    # Healthcare
    'JNJ': 'Johnson & Johnson',
    'UNH': 'UnitedHealth Group Inc.',
    'PFE': 'Pfizer Inc.',
    'ABBV': 'AbbVie Inc.',
    'MRK': 'Merck & Co. Inc.',
    'TMO': 'Thermo Fisher Scientific Inc.',
    'ABT': 'Abbott Laboratories',
    
    # Consumer
    'WMT': 'Walmart Inc.',
    'HD': 'The Home Depot Inc.',
    'MCD': "McDonald's Corporation",
    'NKE': 'Nike Inc.',
    'SBUX': 'Starbucks Corporation',
    'TGT': 'Target Corporation',
    
    # Energy
    'XOM': 'Exxon Mobil Corporation',
    'CVX': 'Chevron Corporation',
    'COP': 'ConocoPhillips',
    'SLB': 'Schlumberger Limited',
    
    # Industrial
    'BA': 'The Boeing Company',
    'CAT': 'Caterpillar Inc.',
    'GE': 'General Electric Company',
    'HON': 'Honeywell International Inc.',
    
    # Communication
    'VZ': 'Verizon Communications Inc.',
    'T': 'AT&T Inc.',
    'CMCSA': 'Comcast Corporation',
    'DIS': 'The Walt Disney Company',
    
    # 기타 주요 종목
    'V': 'Visa Inc.',
    'MA': 'Mastercard Incorporated',
    'PYPL': 'PayPal Holdings Inc.',
    'COST': 'Costco Wholesale Corporation',
    'AVGO': 'Broadcom Inc.',
    'QCOM': 'QUALCOMM Incorporated',
    'TXN': 'Texas Instruments Incorporated',
    'AMAT': 'Applied Materials Inc.',
    'MU': 'Micron Technology Inc.',
    'LRCX': 'Lam Research Corporation',
    'KLAC': 'KLA Corporation',
    'ASML': 'ASML Holding N.V.',
    'NOW': 'ServiceNow Inc.',
    'SNPS': 'Synopsys Inc.',
    'CDNS': 'Cadence Design Systems Inc.',
    'ANET': 'Arista Networks Inc.',
    'PANW': 'Palo Alto Networks Inc.',
    'CRWD': 'CrowdStrike Holdings Inc.',
    'ZS': 'Zscaler Inc.',
    'FTNT': 'Fortinet Inc.',
    'NET': 'Cloudflare Inc.',
    'DDOG': 'Datadog Inc.',
    'MDB': 'MongoDB Inc.',
    'SNOW': 'Snowflake Inc.',
    'PLTR': 'Palantir Technologies Inc.',
    'RBLX': 'Roblox Corporation',
    'U': 'Unity Software Inc.',
    'TTD': 'The Trade Desk Inc.',
    'TTWO': 'Take-Two Interactive Software Inc.',
    'EA': 'Electronic Arts Inc.',
    'ATVI': 'Activision Blizzard Inc.',
    'ZM': 'Zoom Video Communications Inc.',
    'DOCN': 'DigitalOcean Holdings Inc.',
    'GTLB': 'GitLab Inc.',
    'ESTC': 'Elastic N.V.',
    'TEAM': 'Atlassian Corporation',
    'WDAY': 'Workday Inc.',
    'VEEV': 'Veeva Systems Inc.',
    'BILL': 'Bill.com Holdings Inc.',
    'FROG': 'JFrog Ltd.',
    'PATH': 'UiPath Inc.',
    'AI': 'C3.ai Inc.',
    'SOUN': 'SoundHound AI Inc.',
    'SMCI': 'Super Micro Computer Inc.',
    'DELL': 'Dell Technologies Inc.',
    'HPQ': 'HP Inc.',
    'HPE': 'Hewlett Packard Enterprise Company',
    'LEN': 'Lennar Corporation',
    'DHI': 'D.R. Horton Inc.',
    'TOL': 'Toll Brothers Inc.',
    'PHM': 'PulteGroup Inc.',
    'NVR': 'NVR Inc.',
    'RYL': 'Ryland Group Inc.',
    'KBH': 'KB Home',
    'MTH': 'Meritage Homes Corporation',
    'CCL': 'Carnival Corporation & plc',
    'RCL': 'Royal Caribbean Cruises Ltd.',
    'NCLH': 'Norwegian Cruise Line Holdings Ltd.',
    'LVS': 'Las Vegas Sands Corp.',
    'WYNN': 'Wynn Resorts Limited',
    'MGM': 'MGM Resorts International',
    'CZR': 'Caesars Entertainment Inc.',
    'DKNG': 'DraftKings Inc.',
    'PENN': 'Penn National Gaming Inc.',
    'BYD': 'Boyd Gaming Corporation',
    'ERI': 'Eldorado Resorts Inc.',
    'ACHR': 'Archer Aviation Inc.',
    'JOBY': 'Joby Aviation Inc.',
    'LILM': 'Lilium N.V.',
    'EVTL': 'Vertical Aerospace Ltd.',
    'RKT': 'Rocket Companies Inc.',
    'UWMC': 'UWM Holdings Corporation',
    'LOAN': 'Manhattan Bridge Capital Inc.',
    'NRZ': 'New Residential Investment Corp.',
    'TWO': 'Two Harbors Investment Corp.',
    'AGNC': 'AGNC Investment Corp.',
    'NLY': 'Annaly Capital Management Inc.',
    'CIM': 'Chimera Investment Corporation',
    'DX': 'Dynex Capital Inc.',
    'STWD': 'Starwood Property Trust Inc.',
    'BXMT': 'Blackstone Mortgage Trust Inc.',
    'PMT': 'PennyMac Mortgage Investment Trust',
    'NYMT': 'New York Mortgage Trust Inc.',
    'MITT': 'AG Mortgage Investment Trust Inc.',
    'CHMI': 'Cherry Hill Mortgage Investment Corporation',
    'CIM': 'Chimera Investment Corporation',
    'DX': 'Dynex Capital Inc.',
    'STWD': 'Starwood Property Trust Inc.',
    'BXMT': 'Blackstone Mortgage Trust Inc.',
    'PMT': 'PennyMac Mortgage Investment Trust',
    'NYMT': 'New York Mortgage Trust Inc.',
    'MITT': 'AG Mortgage Investment Trust Inc.',
    'CHMI': 'Cherry Hill Mortgage Investment Corporation',
}

def is_valid_ticker(ticker: str) -> bool:
    """티커가 실제 주식 티커인지 확인"""
    return ticker.upper() in MAJOR_STOCK_TICKERS

def get_ticker_name(ticker: str) -> str:
    """티커의 회사명 가져오기"""
    return MAJOR_STOCK_TICKERS.get(ticker.upper(), ticker)

def extract_tickers_from_text(text: str) -> List[str]:
    """텍스트에서 실제 티커 추출"""
    import re
    # 티커 패턴: 대문자 1-5자, 단어 경계
    ticker_pattern = r'\b([A-Z]{1,5})\b'
    potential_tickers = re.findall(ticker_pattern, text.upper())
    
    # 실제 티커만 필터링
    valid_tickers = []
    for ticker in potential_tickers:
        if is_valid_ticker(ticker) and ticker not in valid_tickers:
            valid_tickers.append(ticker)
    
    return valid_tickers
