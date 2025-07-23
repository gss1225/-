import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pykrx import stock
from scipy.optimize import minimize
from utils import adjust_to_business_day
def optimize_portfolio(undervalued_path, lambdas_path):
    undervalued_df = pd.read_excel(undervalued_path)
    undervalued_df['종목코드'] = undervalued_df['종목코드'].astype(str).str.zfill(6)

    assets = undervalued_df['종목코드'].tolist()
    asset_names = dict(zip(undervalued_df['종목코드'], undervalued_df['회사명']))

    lambdas_df = pd.read_csv(lambdas_path)
    lambdas = lambdas_df['lambda'].tolist()

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

    results = []

    def utility(w, mu, Sigma, lam):
        return -(np.dot(w, mu - rf) - lam * np.dot(w, np.dot(Sigma, w)))

    for lam in lambdas:
        result = minimize(
            utility,
            w0,
            args=(mu_vec, Sigma_mat, lam),
            method='SLSQP',
            bounds=bounds,
            constraints=[sum_to_one]
        )
        if not result.success:
            raise RuntimeError(f"λ={lam} 최적화 실패: {result.message}")

        w_opt = result.x
        ret_opt = np.dot(w_opt, mu_vec)
        var_opt = np.dot(w_opt, np.dot(Sigma_mat, w_opt))
        std_opt = np.sqrt(var_opt)
        util_opt = (ret_opt - rf) - lam * var_opt

        results.append({
            'λ': lam,
            '기대수익률': ret_opt,
            '표준편차': std_opt,
            '효용값': util_opt,
            '비중': w_opt
        })

    # 결과 출력
    for res in results:
        print(f"\n=== λ = {res['λ']} ===")
        print(f"기대수익률: {res['기대수익률']:.4%}")
        print(f"표준편차: {res['표준편차']:.4%}")
        print(f"효용값: {res['효용값']:.4f}")
        print("--- 포트폴리오 비중 ---")
        for ticker, weight in zip(assets, res['비중']):
            print(f"{asset_names[ticker]:<20}: {weight:.4f}")

    # 그래프
    weights_matrix = np.array([res['비중'] for res in results])
    lambda_values = [res['λ'] for res in results]

    max_weights = weights_matrix.max(axis=0)
    top5_indices = np.argsort(max_weights)[-5:][::-1]

    # 첫 번째 그래프: λ별 상위 5개 자산 권장 비중
    plt.figure(figsize=(10, 6))
    for i in top5_indices:
        plt.plot(lambda_values, weights_matrix[:, i], marker='o', label=asset_names[assets[i]])

    plt.title("λ별 상위 5개 자산 권장 보유 비중")
    plt.xlabel("λ (위험회피계수)")
    plt.ylabel("자산 비중")
    plt.ylim(0, 1)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("lambda_weights.png")
    plt.show()

    returns_values = [r['기대수익률'] for r in results]
    stddev_values = [r['표준편차'] for r in results]

    # 두 번째 그래프: λ별 기대수익률 및 표준편차
    plt.figure(figsize=(10, 6))
    plt.plot(lambda_values, returns_values, marker='o', label='기대수익률')
    plt.plot(lambda_values, stddev_values, marker='s', label='표준편차')
    plt.title("λ별 기대수익률 및 표준편차")
    plt.xlabel("λ (위험회피계수)")
    plt.ylabel("비율")
    plt.ylim(0, max(max(returns_values), max(stddev_values)) * 1.1)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("lambda_return_risk.png")
    plt.show()
