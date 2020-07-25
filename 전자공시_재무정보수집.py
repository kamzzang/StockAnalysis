import json
import pandas as pd
from urllib.request import urlopen

CRTFC_KEY="5bd9090be03a69214f0b40d260de84e2d3e7daeb"

corp_code="00126380"

bsns_year="2019"

# 11011:사업보고서
reprt_code="11011"

url="https://opendart.fss.or.kr/api/alotMatter.json?crtfc_key={}" \
    "&corp_code={}&bsns_year={}&reprt_code={}".format(CRTFC_KEY,corp_code,bsns_year,reprt_code)


req=urlopen(url)

result=req.read()

result_json=json.loads(result)

print(result_json)