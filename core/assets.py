def get_assets():
    with open('data/asset_list.csv') as f:
        assets = [line.strip().zfill(6) for line in f if line.strip()]
    return assets