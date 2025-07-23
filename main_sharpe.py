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

    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

    from undervalued import find_undervalued_assets
    from portfolio_sharpe import optimize_portfolio_sharpe

    essets_path = r"C:\Users\정상훈\OneDrive\바탕 화면\알고리즘매매\algo_portfolio\essets.csv"
    undervalued_path = r"C:\Users\정상훈\OneDrive\바탕 화면\알고리즘매매\algo_portfolio\undervalued_stocks.xlsx"

    print("🔷 저평가 종목 선별 시작...")
    find_undervalued_assets(essets_path, undervalued_path)

    print("🔷 Sharpe 최적화 포트폴리오 계산 시작...")
    optimize_portfolio_sharpe(undervalued_path)

    print("✅ 작업이 모두 완료되었습니다!")
