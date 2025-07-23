from pykrx import stock
from datetime import datetime
import pandas as pd
import numpy as np
from utils import adjust_to_business_day
import asyncio

async def find_undervalued_assets(input_path, output_path):
    today = datetime.today()
    start_date = (today - pd.DateOffset(years=3)).strftime("%Y%m%d")
    end_date = adjust_to_business_day(today.strftime("%Y%m%d"))

    df = pd.read_csv(input_path, encoding="cp949")
    df["종목코드"] = df["종목코드"].astype(str).str.zfill(6)

    result = []

    async def get_asset(idx, row):
        ticker = row["종목코드"]
        name = row["회사명"]
        try:
            dps_info = get_recent_dps_and_growth(ticker)
            if dps_info is None:
                print(f"{name}({ticker}) 배당 없음, 건너뜀")
                return None

            capm_info = get_capm_required_return(ticker, start_date, end_date)
            fair_value = get_ggm_fair_value(
                dps_info["recent_dps"],
                dps_info["growth"],
                capm_info["required_return"]
            )

            if np.isnan(fair_value):
                print(f"{name}({ticker}) GGM 계산 불가 (r <= g), 건너뜀")
                return None

            price_df = stock.get_market_ohlcv_by_date(end_date, end_date, ticker)
            if price_df.empty:
                print(f"{name}({ticker}) 현재가 데이터 없음, 건너뜀")
                return None

            today_price = price_df["종가"].iloc[0]

            return {
                "회사명": name,
                "종목코드": ticker,
                "적정주가": round(fair_value, 2),
                "현재주가": today_price,
                "저평가여부": today_price < fair_value
            }
        except Exception as e:
            print(f"{name}({ticker}) 처리 중 오류: {e}")

    results = await asyncio.gather(*[get_asset(idx, row) for idx, row in df.iterrows()])
    
    for r in results:
        if r is not None:
            result.append(r) 
        

    result_df = pd.DataFrame(result)
    undervalued_df = result_df[result_df["저평가여부"]]

    undervalued_df.to_excel(output_path, index=False)
    undervalued_df.to_csv(output_path.replace(".xlsx", ".csv"), index=False)

    print(f"저평가 종목 파일 저장 완료: {output_path}")

def get_recent_dps_and_growth(ticker: str):
    current_year = datetime.today().year
    years = [current_year - 1, current_year - 2]

    dps_list = []
    for year in years:
        query_year = year + 1
        date = adjust_to_business_day(f"{query_year}0401")
        fund = stock.get_market_fundamental(date)

        try:
            dps = fund.loc[ticker, "DPS"]
            if pd.isna(dps):
                dps = 0.0
        except KeyError:
            dps = 0.0

        dps_list.append((year, dps))

    recent_year, recent_dps = dps_list[0]
    prev_year, prev_dps = dps_list[1]

    if recent_dps == 0.0:
        return None

    if prev_dps > 0:
        g = (recent_dps / prev_dps) - 1
    else:
        g = 0.0  # 처음 배당 시작한 경우

    return {
        "recent_year": recent_year,
        "recent_dps": recent_dps,
        "prev_year": prev_year,
        "prev_dps": prev_dps,
        "growth": g
    }

def get_capm_required_return(ticker: str, start_date: str, end_date: str, rf: float = 0.03):
    stock_price = stock.get_market_ohlcv(start_date, end_date, ticker)["종가"]
    stock_ret = stock_price.pct_change().dropna()

    kospi_price = stock.get_index_ohlcv(start_date, end_date, "1001")["종가"]
    kospi_ret = kospi_price.pct_change().dropna()

    aligned = stock_ret.to_frame("stock").join(kospi_ret.to_frame("market")).dropna()

    mkt_mean_daily = aligned["market"].mean()
    mkt_annual = mkt_mean_daily * 252

    cov = np.cov(aligned["stock"], aligned["market"])[0, 1]
    var_mkt = aligned["market"].var()
    beta = cov / var_mkt

    required_return = rf + beta * (mkt_annual - rf)

    return {
        "beta": beta,
        "market_return": mkt_annual,
        "required_return": required_return
    }

def get_ggm_fair_value(recent_dps: float, g: float, r: float):
    if r <= g:
        print("⚠️ r <= g 이므로 GGM 계산 불가")
        return np.nan

    fair_value = recent_dps * (1 + g) / (r - g)
    return fair_value


import subprocess
import sys
import os

def install_requirements():
    """
    필요한 패키지들을 한 번에 설치
    """
    packages = [
        "numpy",
        "pandas",
        "matplotlib",
        "scipy",
        "pykrx",
        "openpyxl"
    ]
    try:
        print(f"🔷 다음 패키지를 설치합니다: {', '.join(packages)}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", *packages])
    except subprocess.CalledProcessError as e:
        print(f"패키지 설치 중 오류 발생: {e}")
        raise

if __name__ == "__main__":
    print("🔷 필요한 패키지 설치 시작...")
    install_requirements()

    # 현재 폴더를 sys.path에 추가
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

    # from undervalued import find_undervalued_assets
    from portfolio import optimize_portfolio

    essets_path = r"C:\Users\정상훈\OneDrive\바탕 화면\알고리즘매매\algo_portfolio\essets.csv"
    undervalued_path = r"C:\Users\정상훈\OneDrive\바탕 화면\알고리즘매매\algo_portfolio\undervalued_stocks2.xlsx"
    lambdas_path = r"C:\Users\정상훈\OneDrive\바탕 화면\알고리즘매매\algo_portfolio\lambdas.csv"

    print("🔷 저평가 종목 선별 시작...")
    asyncio.run(find_undervalued_assets(essets_path, undervalued_path))

    print("🔷 포트폴리오 최적화 시작...")
    optimize_portfolio(undervalued_path, lambdas_path)

    print("✅ 작업이 모두 완료되었습니다!")
