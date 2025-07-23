import requests

API_KEY = "c30d0ce9fa8a3d638d4751c6cec5d6087feacf07"

url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={API_KEY}"
response = requests.get(url)

if response.status_code == 200:
    with open("corpCode.xml", "wb") as f:
        f.write(response.content)
    print("✅ corpCode.xml 다운로드 완료")
else:
    print(f"🚨 오류: {response.status_code}")
    print(response.text)

import zipfile

with open("corpCode.zip", "wb") as f:
    f.write(response.content)

with zipfile.ZipFile("corpCode.zip", "r") as zip_ref:
    zip_ref.extractall("corpCode_extracted")

print("✅ 다운로드 및 압축해제 완료")
