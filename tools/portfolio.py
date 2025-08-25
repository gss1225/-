import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime
from scipy.optimize import minimize
from sqlite3 import Connection

from core import database
from tools.utils import to_df


def negative_sharpe_ratio(w, mu, Sigma, rf):
    ret = np.dot(w, mu)
    vol = np.sqrt(np.dot(w, np.dot(Sigma, w)))
    sharpe = (ret - rf) / vol
    return -sharpe

def get_returns(conn: Connection, assets: list[str], start: str, end: str) -> pd.DataFrame:
    price_df = pd.DataFrame()
    for stock_code in assets:
        close_prices = to_df(database.fetch_stock_day_by_stock(conn, stock_code, start, end), 'date', ['close_price'])
        price_df[stock_code] = close_prices
    price_df = price_df.dropna(axis=1, how='all')
    returns = price_df.pct_change().dropna()
    return returns

def get_lambda_result(lam, mu_vec, Sigma_mat, w0, bounds, constraints, rf):
    def utility(w, mu, Sigma, lam):
        return -(np.dot(w, mu - rf) - lam * np.dot(w, np.dot(Sigma, w)))
    
    result = minimize(
        utility,
        w0,
        args=(mu_vec, Sigma_mat, lam),
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )
    
    w_opt = result.x
    ret_opt = np.dot(w_opt, mu_vec)
    var_opt = np.dot(w_opt, np.dot(Sigma_mat, w_opt))
    std_opt = np.sqrt(max(var_opt, 0.0))  # numerical guard
    util_opt = (ret_opt - rf) - lam * var_opt

    return result, (w_opt, ret_opt, var_opt, std_opt, util_opt)

def get_sharpe_result(mu_vec, Sigma_mat, w0, bounds, constraints, rf):
    result = minimize(
        negative_sharpe_ratio,
        w0,
        args=(mu_vec, Sigma_mat, rf),
        method='SLSQP',
        bounds=bounds,
        constraints=constraints
    )
    
    w_opt = result.x
    ret_opt = np.dot(w_opt, mu_vec)
    var_opt = np.dot(w_opt, np.dot(Sigma_mat, w_opt))
    std_opt = np.sqrt(var_opt)
    sharpe_opt = (ret_opt - rf) / std_opt

    return result, (w_opt, ret_opt, var_opt, std_opt, sharpe_opt)

def optimize_portfolio(conn: Connection, assets: list[str], lambdas: list[float], start: datetime, end: datetime, rf=0.03):
    returns = get_returns(conn, assets, start, end)
    if returns.empty or len(returns.columns) == 0:
        raise ValueError("수익률 데이터가 비었습니다. 입력 종목/기간을 확인하세요.")

    mu = returns.mean() * 252
    Sigma = returns.cov() * 252

    mu_vec = mu.values
    Sigma_mat = Sigma.values
    # numerical stability: ensure covariance is positive semi-definite  <- gpt가 뭐시기 해줌
    if Sigma_mat.size:
        Sigma_mat = Sigma_mat + 1e-8 * np.eye(Sigma_mat.shape[0])

    used_assets = list(returns.columns)
    n = len(used_assets)
    w0 = np.ones(n) / n
    bounds = tuple((0, 1) for _ in range(n))
    sum_to_one = {'type': 'eq', 'fun': lambda w: np.sum(w) - 1}

    lambda_results = []
    for lam in lambdas:
        result, (w_opt, ret_opt, var_opt, std_opt, util_opt) = get_lambda_result(lam, mu_vec, Sigma_mat, w0, bounds, [sum_to_one], rf)
        if not result.success:
            print(f"λ={lam} 최적화 실패: {result.message}")
            continue

        lambda_results.append({
            'λ': lam,
            '기대수익률': ret_opt,
            '표준편차': std_opt,
            '효용값': util_opt,
            '비중': w_opt
        })

    result, (w_opt_sharpe, ret_opt_sharpe, var_opt_sharpe, std_opt_sharpe, sharpe_opt) = get_sharpe_result(mu_vec, Sigma_mat, w0, bounds, [sum_to_one], rf)
    if not result.success:
        print(f"sharpe 최적화 실패: {result.message}")
    
    sharpe = {
        '기대수익률': ret_opt_sharpe,
        '표준편차': std_opt_sharpe,
        'Sharpe 비율': sharpe_opt,
        '비중': w_opt_sharpe
    }

    return {
        'lambda_results': lambda_results,
        'sharpe': sharpe,
        'stock_codes': used_assets,
        'mu': mu,
        'Sigma': Sigma,
        'returns': returns,
    }

def graph_lambda(conn, results, assets):
    names = to_df(database.fetch_companies(conn, assets), columns=['name'])

    # 결과 출력
    for res in results:
        print(f"\n=== λ = {res['λ']} ===")
        print(f"기대수익률: {res['기대수익률']:.4%}")
        print(f"표준편차: {res['표준편차']:.4%}")
        print(f"효용값: {res['효용값']:.4f}")
        print("--- 포트폴리오 비중 ---")
        for stock_code, weight in zip(assets, res['비중']):
            print(f"{names[stock_code]:<20}: {weight:.4f}")

    # 그래프
    weights_matrix = np.array([res['비중'] for res in results])
    lambda_values = [res['λ'] for res in results]

    max_weights = weights_matrix.max(axis=0)
    top5_indices = np.argsort(max_weights)[-5:][::-1]

    # 첫 번째 그래프: λ별 상위 5개 자산 권장 비중
    plt.figure(figsize=(10, 6))
    for i in top5_indices:
        plt.plot(lambda_values, weights_matrix[:, i], marker='o', label=names[assets[i]])

    plt.title("λ별 상위 5개 자산 권장 보유 비중")
    plt.xlabel("λ (위험회피계수)")
    plt.ylabel("자산 비중")
    plt.ylim(0, 1)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("results/lambda_weights.png")
    # plt.show()

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
    plt.savefig("results/lambda_return_risk.png")
    # plt.show()


def graph_sharpe(conn, result, assets):
    names = to_df(database.fetch_companies(conn, assets), columns=['name'])
    
    print(f"\n=== Sharpe 비율 최적화 결과 ===")
    print(f"기대수익률: {result['기대수익률']:.4%}")
    print(f"표준편차: {result['표준편차']:.4%}")
    print(f"Sharpe 비율: {result['Sharpe 비율']:.4f}")
    print("--- 포트폴리오 비중 ---")
    sorted_results = sorted(
        ((stock_code, weight) for stock_code, weight in zip(assets, result['비중']) if weight >= 0.0001),
        key=lambda x: x[1],
        reverse=True
    )

    sorted_names = []
    sorted_weights = []
    for stock_code, weight in sorted_results:
        print(f"{names[stock_code]:<20}: {weight:.4f}")
        sorted_names.append(names[stock_code])
        sorted_weights.append(weight)

    print(type(sorted_results[1][0]))

    plt.figure(figsize=(10, 6))
    plt.bar(sorted_names, sorted_weights)
    plt.title("Sharpe 최적화 포트폴리오 자산 비중")
    plt.ylabel("비중")
    plt.ylim(0, 1)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig("results/sharpe_optimal_weights.png")
    # plt.show()