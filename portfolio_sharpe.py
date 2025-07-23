import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pykrx import stock
from scipy.optimize import minimize
from utils import adjust_to_business_day

def optimize_portfolio_sharpe(undervalued_path):
    undervalued_df = pd.read_excel(undervalued_path)
    undervalued_df['종목코드'] = undervalued_df['종목코드'].astype(str).str.zfill(6)

    assets = undervalued_df['종목코드'].tolist()
    asset_names = dict(zip(undervalued_df['종목코드'], undervalued_df['회사명']))

    end_date = datetime.today()
    start_date = end_date - timedelta(days=365*3)
    start = start_date.strftime("%Y%m%d")
    end = adjust_to_business_day(end_date.strftime("%Y%m%d"))

    price_df = pd.DataFrame()
    for ticker in assets:
        try:
            df = stock.get_market_ohlcv(start, end, ticker)["종가"]
            price_df[ticker] = df
        except:
            print(f"데이터 수집 실패: {ticker}")

    returns = price_df.pct_change().dropna()
    mu = returns.mean() * 252
    Sigma = returns.cov() * 252

    mu_vec = mu.values
    Sigma_mat = Sigma.values

    rf = 0.03
    n = len(assets)
    w0 = np.ones(n) / n
    bounds = tuple((0, 1) for _ in range(n))
    sum_to_one = {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}

    def negative_sharpe_ratio(w, mu, Sigma, rf):
        ret = np.dot(w, mu)
        vol = np.sqrt(np.dot(w, np.dot(Sigma, w)))
        sharpe = (ret - rf) / vol
        return -sharpe

    result = minimize(
        negative_sharpe_ratio,
        w0,
        args=(mu_vec, Sigma_mat, rf),
        method='SLSQP',
        bounds=bounds,
        constraints=[sum_to_one]
    )

    if not result.success:
        raise RuntimeError(f"Sharpe 최적화 실패: {result.message}")

    w_opt = result.x
    ret_opt = np.dot(w_opt, mu_vec)
    var_opt = np.dot(w_opt, np.dot(Sigma_mat, w_opt))
    std_opt = np.sqrt(var_opt)
    sharpe_opt = (ret_opt - rf) / std_opt

    print(f"\n=== Sharpe 비율 최적화 결과 ===")
    print(f"기대수익률: {ret_opt:.4%}")
    print(f"표준편차: {std_opt:.4%}")
    print(f"Sharpe 비율: {sharpe_opt:.4f}")
    print("--- 포트폴리오 비중 ---")
    for ticker, weight in zip(assets, w_opt):
        print(f"{asset_names[ticker]:<20}: {weight:.4f}")

    plt.figure(figsize=(10, 6))
    plt.bar([asset_names[t] for t in assets], w_opt)
    plt.title("Sharpe 최적화 포트폴리오 자산 비중")
    plt.ylabel("비중")
    plt.ylim(0, 1)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("sharpe_optimal_weights.png")
    plt.show()
