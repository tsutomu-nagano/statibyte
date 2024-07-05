import logging
from pathlib import Path
import re
import time
import os
import unicodedata
import json
import csv

import requests
import pandas as pd
import sys

from estat_api import *


appid = os.environ["APPID"]
root_dir = Path(os.environ["ROOT_DIR"])

# 処理対象データをパラメータで受け取る
args = sys.argv
src = args[1]
# src = root_dir / "table_of_1day.csv"

# メタのフォルダ作成
meta_dir = root_dir / "meta"
meta_dir.mkdir(parents=True, exist_ok = True)


ids = pd.read_csv(src, dtype = str, usecols = ["@id"])["@id"].tolist()

for id in ids[1:2]:
    dest = meta_dir / f"{id}.csv"
    create_meta_list(appid, id).to_csv(dest, index = False, quoting=csv.QUOTE_ALL)


