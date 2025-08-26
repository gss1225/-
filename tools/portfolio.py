from unittest import result
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from datetime import datetime
from scipy.optimize import minimize
from sqlite3 import Connection

from core import database
from tools.utils import to_df

from core.logger import get_logger
logger = get_logger(__name__)

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

def optimize_portfolio(conn: Connection, assets: list[str], lambdas: list[float], start_date: datetime, end_date: datetime, rf=0.03):
    start = start_date.strftime('%Y%m%d')
    end = end_date.strftime('%Y%m%d')
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
            logger.info(f"λ={lam} 최적화 실패: {result.message}")
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
        logger.info(f"sharpe 최적화 실패: {result.message}")
    
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
    }

def graph_lambda(conn, results, assets):
    today = datetime.now().strftime('%Y%m%d')
    names = to_df(database.fetch_all_companies(conn), 'stock_code')['name']

    # 결과 출력
    # for res in results:
    #     logger.info(f"\n=== λ = {res['λ']} ===")
    #     logger.info(f"기대수익률: {res['기대수익률']:.4%}")
    #     logger.info(f"표준편차: {res['표준편차']:.4%}")
    #     logger.info(f"효용값: {res['효용값']:.4f}")
    #     logger.info("--- 포트폴리오 비중 ---")
        
    #     sorted_results = sorted(
    #         ((stock_code, weight) for stock_code, weight in zip(assets, res['비중']) if weight >= 0.0001),
    #         key=lambda x: x[1],
    #         reverse=True
    #     )
    #     for stock_code, weight in sorted_results:
    #         logger.info(f"{names.loc[stock_code]:<20}: {weight:.4f}")


    # 그래프
    weights_matrix = np.array([res['비중'] for res in results])
    lambda_values = [res['λ'] for res in results]

    max_weights = weights_matrix.max(axis=0)
    top5_indices = np.argsort(max_weights)[-5:][::-1]

    # Prepare values for both graphs
    returns_values = [r['기대수익률'] for r in results]
    stddev_values = [r['표준편차'] for r in results]

    # Create a single figure with two subplots (vertically stacked)
    fig, axes = plt.subplots(2, 1, figsize=(10, 12))

    # 첫 번째 그래프: λ별 상위 5개 자산 권장 비중
    ax1 = axes[0]
    for i in top5_indices:
        ax1.plot(lambda_values, weights_matrix[:, i], marker='o', label=names[assets[i]])
    ax1.set_title("λ별 상위 5개 자산 권장 보유 비중")
    ax1.set_xlabel("λ (위험회피계수)")
    ax1.set_ylabel("자산 비중")
    ax1.set_ylim(0, 1)
    ax1.legend()
    ax1.grid(True)

    # 두 번째 그래프: λ별 기대수익률 및 표준편차
    ax2 = axes[1]
    ax2.plot(lambda_values, returns_values, marker='o', label='기대수익률')
    ax2.plot(lambda_values, stddev_values, marker='s', label='표준편차')
    ax2.set_title("λ별 기대수익률 및 표준편차")
    ax2.set_xlabel("λ (위험회피계수)")
    ax2.set_ylabel("비율")
    ax2.set_ylim(0, max(max(returns_values), max(stddev_values)) * 1.1)
    ax2.legend()
    ax2.grid(True)

    plt.tight_layout()
    plt.savefig(f"results/lambda/lambda_{today}.png")
    # plt.show()


def graph_sharpe(conn, result, assets):
    today = datetime.now().strftime('%Y%m%d')
    names = to_df(database.fetch_companies(conn, assets), 'stock_code')['name']
    
    logger.info(f"\n=== Sharpe 비율 최적화 결과 ===")
    logger.info(f"기대수익률: {result['기대수익률']:.4%}")
    logger.info(f"표준편차: {result['표준편차']:.4%}")
    logger.info(f"Sharpe 비율: {result['Sharpe 비율']:.4f}")
    logger.info("--- 포트폴리오 비중 ---")
    sorted_results = sorted(
        ((stock_code, weight) for stock_code, weight in zip(assets, result['비중']) if weight >= 0.0001),
        key=lambda x: x[1],
        reverse=True
    )

    sorted_names = []
    sorted_weights = []
    for stock_code, weight in sorted_results:
        logger.info(f"{names.loc[stock_code]:<20}: {weight:.4f}")
        sorted_names.append(names.loc[stock_code])
        sorted_weights.append(weight)

    plt.figure(figsize=(10, 6))
    plt.bar(sorted_names, sorted_weights)
    plt.title("Sharpe 최적화 포트폴리오 자산 비중")
    plt.ylabel("비중")
    plt.ylim(0, 1)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(f"results/sharpe/sharpe_{today}.png")
    # plt.show()