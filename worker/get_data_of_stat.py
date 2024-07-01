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

mm_of_q = {
    "1":"0103",
    "2":"0406",
    "3":"0709",
    "4":"1012",
    "１":"0103",
    "２":"0406",
    "３":"0709",
    "４":"1012",

}

yyyy_of_era = {
    "明治":1867,
    "大正":1911,
    "昭和":1925,
    "平成":1988,
    "令和":2018,
    "明":1867,
    "大":1911,
    "昭":1925,
    "平":1988,
    "令":2018,
}

GAISAN_end_pth = re.compile("^(?P<e>.+)([（\\()]概(算)?[）\\)])$")
COMMENT_end_ptn = re.compile("^(?P<e>.+)([（\\()]\\*[0-9]+[）\\)])$")
NEWOLD_end_ptn = re.compile("^(?P<e>.+)([（\\()][新旧][）\\)])$")
BEFORAFTER_end_ptn = re.compile("^(?P<e>.+)([（\\()]変更[前後][）\\)])$")
P_end_ptn = re.compile("^(?P<e>.+)p$")
OTHER_end_ptn1 = re.compile("^(?P<e>.+)([（\\()](専用船含む推計|組替|再集計(後)?|税制改正前|原数値|ソフトウェア業を(含まない|含む))[）\\)])$")
OTHER_end_ptn2 = re.compile("^(?P<e>.+)(_不詳補完値|_外数|（組替）_人口５万(以上|未満)の市町村)$")
OTHER_end_ptn3 = re.compile("^(?P<e>.+)(\\(13年基準\\)|1994年調査調査と.+|_人口５万(以上|未満)の市町村|（平成.+全国消費.+)$")

REGION_start_ptn = re.compile("^(.+[都道府県]|東北|北陸|関東・東山|東海|近畿|中国|四国|九州)_(?P<e>.+)$")
TIMEKIND_start_ptn = re.compile("^(年度|年次)_(?P<e>.+)$")
OTHER_start_ptn = re.compile("^(田畑計|田作|畑作)_(?P<e>.+)$")


yyyy_NEN_ptn = re.compile("^(?P<yyyy>[0-9]{4})(年(度)?(末)?(間)?)?$")
yyyy_NEN_box_ptn = re.compile("^\\((?P<yyyy>[0-9]{4})年\\)$")

yyyy_NEN_mm_TSUKI_ptn = re.compile("^(?P<yyyy>[0-9]{4})年(?P<mm>[0-9０-９]+)月(末)?$")
yyyy_NEN_mm_TSUKI_dd_HI_ptn = re.compile("^(?P<yyyy>[0-9]{4})年(?P<mm>[0-9０-９]+)月(末)?(?P<dd>[0-9０-９]+)日(現在)?$")

yyyy_NEN_yyyy_NEN_mm_TSUKI_dd_HI_ptn = re.compile("^[0-9]{4}年\\((?P<yyyy>[0-9]{4})年(?P<mm>[0-9０-９]+)月(末)?(?P<dd>[0-9０-９]+)日(現在)?\\)$")


yyyy_NEN_q_TSUKI_ptn = re.compile("^(?P<yyyy>[0-9]{4})年(度)?(?P<mm_from>[0-9]+)(月)?[～\\-~](?P<mm_to>[0-9]+)月(期)?$")
yyyy_NEN_withERA_ptn = re.compile("^(?P<yyyy>[0-9０-９]{4})(年(度)?)?[（\\(]平成[0-9]+年(度)?[）\\)]$")
yyyy_NEN_withERASHORT_ptn = re.compile("^(?P<yyyy>[0-9０-９]{4})(年(度)?)?[（\\(]平[．\\.][0-9]+(年)?[）\\)]$")

yyyy_q_ptn = re.compile("^(?P<yyyy>[0-9]{4})_(?P<q>[1234])Q")
yyyy_SLASH_mm_ptn = re.compile("^(?P<yyyy>[0-9]{4})/(?P<mm>[0-9]+)$")



ERA_yy_NEN_ptn = re.compile("^(?P<era>(明治|大正|昭和|平成|令和))(\\()?(?P<yy>([0-9０-９]+|元))(\\))?(会計)?年([産度末])?$")
ERA_yy_NEN_mm_TSUKI_ptn = re.compile("^(?P<era>(明治|大正|昭和|平成|令和))(?P<yy>([0-9０-９]+|元))年([産度])?(?P<mm>[0-9０-９]+)月$")
ERA_yy_NEN_q_TSUKI_ptn = re.compile("^(?P<era>(明治|大正|昭和|平成|令和))(?P<yy>([0-9０-９]+|元))年([産度])?(?P<mm_from>[0-9]+)[～\\-](?P<mm_to>[0-9]+)月(期)?$")
ERA_yy_NEN_q_ptn = re.compile("^(?P<era>(明治|大正|昭和|平成|令和))(?P<yy>([0-9０-９]+|元))年([産度])?(?P<q>[1234１２３４])期?$")
ERA_yy_NEN_mm_TSUKI_dd_HI_ptn = re.compile("^(?P<era>(明治|大正|昭和|平成|令和))(?P<yy>([0-9０-９]+|元))年(?P<mm>[0-9０-９]+)月(末)?(?P<dd>[0-9０-９]+)日(現在)?$")

ERA_yy_NEN_yyyy_NEN_ptn = re.compile("^(?P<era>(明治|大正|昭和|平成|令和))(?P<yy>([0-9０-９]+|元))(会計)?年([産度末])?[（\\()](?P<yyyy>[0-9]{4})年[）\\)]$")
yyyy_NEN_ERA_yy_NEN_ptn = re.compile("^(?P<yyyy>[0-9]{4})年[（\\()](?P<era>(明治|大正|昭和|平成|令和))(?P<yy>([0-9０-９]+|元))年[）\\)]$")



ERASHORT_DOT_yy_NEN_ptn = re.compile("^(?P<era>(明|大|昭|平|令))[\\.．](?P<yy>([0-9０-９]+|元))年([産度])?$")
ERASHORT_DOT_yy_ptn = re.compile("^(?P<era>(明|大|昭|平|令))[\\.．](?P<yy>([0-9０-９]+|元))$")
ERASHORT_DOT_yy_mm_ptn = re.compile("^(?P<era>(明|大|昭|平|令))[\\.．](?P<yy>([0-9０-９]+|元))[\\.．](?P<mm>([0-9]+))$")
ERASHORT_DOT_yy_NEN_mm_TSUKI_ptn = re.compile("^(?P<era>(明|大|昭|平|令))[\\.．](?P<yy>([0-9０-９]+|元))年([産度])?(?P<mm>([0-9]+))月$")


UNKNOWN_ptn1 = re.compile("^[0-9]+年産$")
UNKNOWN_ptn2 = re.compile("^[0-9]+年[０-９0-9]+月(以[前降])?$")
UNKNOWN_ptn3 = re.compile("^[^0-9]+$")



# logfile = os.environ["LOGFILE"]
# Path(logfile)
# os.remove(logfile)

retry_count = 3

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s:%(name)s - %(message)s", filename="test.log")






    # pd.concat([pd.json_normalize(table_inf).astype(str) for table_inf in table_infs]).to_parquet(f"table/{statcode}.parquet")





def create_meta_list(statcode: str, statdisp_id, overwrite = False):

    dest = Path("meta") / statcode / f"{statdisp_id}.parquet"

    if not dest.exists() or overwrite:

        retry = 1
        state = 504
        while retry <= retry_count and state == 504: 
            logging.info(f"{statdisp_id} retry = {retry}")
            url = f"http://api.e-stat.go.jp/rest/3.0/app/json/getMetaInfo?appId={appid}&statsDataId={statdisp_id}"
            res = requests.get(url)

            state = res.status_code

            if res.status_code == 504:
                time.sleep(15)
                retry += 1

        result = res.json()["GET_META_INFO"]["RESULT"]

        if result["STATUS"] != 0:
            print(result)

        class_objs = to_list(res.json()["GET_META_INFO"]["METADATA_INF"]["CLASS_INF"]["CLASS_OBJ"])

        dfs = [class_to_df(class_obj) for class_obj in class_objs if "CLASS" in class_obj]
        
        if len(dfs) >= 1:
            pd.concat(dfs).assign(statdisp_id = statdisp_id).to_parquet(str(dest))
        else:
            logging.info(f"{statdisp_id} None Meta")

    else:
        logging.info(f"{statdisp_id} file exist none request")


def create_meta_list_all():

    for tablefile in list(Path("table").glob("*")):

        statcode = re.match("(?P<statcode>[0-9]+)", tablefile.stem).group("statcode")

        logging.info(f"{statcode} create meta list")

        dest_dir = Path("meta") / statcode
        dest_dir.mkdir(exist_ok = True)

        [create_meta_list(statcode, statdisp_id) for statdisp_id in pd.read_parquet(tablefile, columns = ["@id"])["@id"].values.tolist()]


# statcode = "00550040"
# statdisp_id = "0003105743"
# create_meta_list(statcode, statdisp_id)

# create_meta_list_all()

def create_meta_item_code(statcode):
    meta_root = Path("meta")
    meta_root.mkdir(exist_ok=True)

    meta_dir = Path("meta_work") / statcode

    df = pd.concat([pd.read_parquet(f, columns = ["@code","@name", "meta_name","id"]).replace({"id":{"cat[0-9]+":"cat"}}, regex = True) for f in list(meta_dir.glob("*"))]
            ).drop_duplicates(
            )


    dest = meta_root / f"{statcode}_item.parquet"
    item = df[["id","meta_name"]].drop_duplicates().assign(num = lambda df: list(range(1, len(df) + 1)))
    item.to_parquet(dest)

    dest = meta_root / f"{statcode}_code.parquet"
    code = df.assign(num = list(range(1, len(df) + 1)))
    code.to_parquet(dest)


def create_meta_item_code_all():

    meta_work_root = Path("meta_work")

    [create_meta_item_code(meta_dir.stem) for meta_dir in meta_work_root.glob("*")]



# create_meta_item_code_all()


def create_meta_ref(statcode):

    dest_root = Path("ref")
    dest_root.mkdir(exist_ok= True)

    src = f"meta/{statcode}_item.parquet"
    item = pd.read_parquet(src)

    src = Path("meta_work") / statcode
    base = pd.concat([pd.read_parquet(file, columns = ["id","meta_name","statdisp_id"]).drop_duplicates() for file in src.glob("*.*")])

    dest = dest_root / f"{statcode}.parquet"
    pd.merge(base, item, on = ["id","meta_name"])[["statdisp_id", "num"]].to_parquet(dest)



def statcodes():
    return([file.stem for file in Path("table").glob("*")])

def create_meta_ref_all():

    meta_work_root = Path("meta_work")

    [create_meta_ref(meta_dir.stem) for meta_dir in meta_work_root.glob("*")]


def concat(root):
    return(pd.concat([pd.read_parquet(file) for file in Path(root).glob("*.*")]))



def create_time():
    [pd.read_parquet(file).assign(statcode = file.stem).pipe(lambda df: df[df["id"] == "time"]).to_parquet(f"time/{file.name}") for file in Path("meta").glob("*code*")]


def era2y(era, yy):
    if yy == "元":
        yy = "1"
    else:
        yy = unicodedata.normalize('NFKC', yy)

    base = yyyy_of_era[era]

    return(str(base + int(yy)))


def pre_format(e):
    # 間のスペースは全て削除
    e = re.sub("\s+","", e)

    # 末尾の（概算）or（概）は削除
    mat = GAISAN_end_pth.match(e)
    if not mat is None:
        e = mat.group("e")

    # 末尾の注釈記号（ (*1)とか ）は削除
    mat = COMMENT_end_ptn.match(e)
    if not mat is None:
        e = mat.group("e")

    # 末尾の新旧の表記は削除
    mat = NEWOLD_end_ptn.match(e)
    if not mat is None:
        e = mat.group("e")

    # 末尾の変更前、変更後の表記は削除
    mat = BEFORAFTER_end_ptn.match(e)
    if not mat is None:
        e = mat.group("e")

    # 末尾の速報記号（p）の表記は削除
    mat = P_end_ptn.match(e)
    if not mat is None:
        e = mat.group("e")

    # 先頭の都道府県等の地域の表記は削除
    mat = REGION_start_ptn.match(e)
    if not mat is None:
        e = mat.group("e")

    # 先頭の時間軸種類の表記は削除
    mat = TIMEKIND_start_ptn.match(e)
    if not mat is None:
        e = mat.group("e")



    # 末尾のその他削除が必要な表記
    mat = OTHER_end_ptn1.match(e)
    if not mat is None:
        e = mat.group("e")

    mat = OTHER_end_ptn2.match(e)
    if not mat is None:
        e = mat.group("e")

    mat = OTHER_end_ptn3.match(e)
    if not mat is None:
        e = mat.group("e")


    # 先頭のその他削除が必要な表記
    mat = OTHER_start_ptn.match(e)
    if not mat is None:
        e = mat.group("e")


    return(e)

def convert_yyyy_mm(e):

    mat = yyyy_NEN_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), "00", "0000")

    mat = yyyy_NEN_box_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), "00", "0000")


    mat = yyyy_NEN_withERA_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), "00", "0000")

    mat = yyyy_NEN_withERASHORT_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), "00", "0000")

    mat = yyyy_SLASH_mm_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), mat.group("mm").zfill(2), "0000")


    mat = yyyy_q_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), "00", mm_of_q[mat.group("q")])



    mat = ERA_yy_NEN_ptn.match(e)
    if not mat is None:
        return(era2y(mat.group("era"), mat.group("yy")), "00", "0000")


    mat = ERA_yy_NEN_yyyy_NEN_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), "00", "0000")

    mat = yyyy_NEN_ERA_yy_NEN_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), "00", "0000")



    mat = yyyy_NEN_mm_TSUKI_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), mat.group("mm").zfill(2),"0000")

    mat = yyyy_NEN_mm_TSUKI_dd_HI_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), mat.group("mm").zfill(2),"0000")


    mat = yyyy_NEN_yyyy_NEN_mm_TSUKI_dd_HI_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), mat.group("mm").zfill(2),"0000")


    mat = yyyy_NEN_q_TSUKI_ptn.match(e)
    if not mat is None:
        return(mat.group("yyyy"), "00", mat.group("mm_from").zfill(2) + mat.group("mm_to").zfill(2))

    mat = ERASHORT_DOT_yy_NEN_ptn.match(e)
    if not mat is None:
        return(era2y(mat.group("era"), mat.group("yy")), "00", "0000")

    mat = ERASHORT_DOT_yy_ptn.match(e)
    if not mat is None:
        return(era2y(mat.group("era"), mat.group("yy")), "00", "0000")

    mat = ERASHORT_DOT_yy_mm_ptn.match(e)
    if not mat is None:
        return(era2y(mat.group("era"), mat.group("yy")), mat.group("mm").zfill(2), "0000")

    mat = ERASHORT_DOT_yy_NEN_mm_TSUKI_ptn.match(e)
    if not mat is None:
        return(era2y(mat.group("era"), mat.group("yy")), mat.group("mm").zfill(2), "0000")




    mat = ERA_yy_NEN_mm_TSUKI_ptn.match(e)
    if not mat is None:
        return(era2y(mat.group("era"), mat.group("yy")), mat.group("mm").zfill(2),"0000")

    mat = ERA_yy_NEN_mm_TSUKI_dd_HI_ptn.match(e)
    if not mat is None:
        return(era2y(mat.group("era"), mat.group("yy")), mat.group("mm").zfill(2),"0000")


    mat = ERA_yy_NEN_q_TSUKI_ptn.match(e)
    if not mat is None:
        return(era2y(mat.group("era"), mat.group("yy")), "00", mat.group("mm_from").zfill(2) +mat.group("mm_to").zfill(2))

    mat = ERA_yy_NEN_q_ptn.match(e)
    if not mat is None:
        return(era2y(mat.group("era"), mat.group("yy")), "00", mm_of_q[mat.group("q")])


    mat = UNKNOWN_ptn1.match(e)
    if not mat is None:
        return("0000","00","0000")

    mat = UNKNOWN_ptn2.match(e)
    if not mat is None:
        return("0000","00","0000")

    mat = UNKNOWN_ptn3.match(e)
    if not mat is None:
        return("0000","00","0000")



    return("","", "")



# create_time()


# src = "/code/ref/00100103.parquet"
# ref = pd.read_parquet(src).pipe(
#         lambda df: df[df["statdisp_id"] == "0003339849"]
#         )

# concat("meta_work/00100103") 


# print(ref)
# print(xx)

# survey_dates = concat("table")[["@id","SURVEY_DATE"]]


# print(survey_dates)

# df = concat("time")

# df["@name_"] = df["@name"].map(pre_format)

# df[["yyyy","mm","q"]] = df["@name_"].map(convert_yyyy_mm).tolist()

# # print(df[df["q"] != "0000"])


# df_ = df[df["yyyy"] == ""]


# df_.to_csv("noset.csv")

# print(df_)

# df_ = df.assign(yyyy = "0"
#         ).assign(yyyy = lambda df: df["yyyy"].mask(df["@name"].str.contains("[0-9]{4}年"), df["@name"].str[0:4])
#         ).assign(yyyy = lambda df: df["yyyy"].mask(df["@name"].str.contains("[0-9]{4}年"), df["@name"].map()   )
#         )

# print(df_[df_["yyyy"] == "0"])

appid = os.environ["APPID"]
root_dir = Path(os.environ["ROOT_DIR"])

# 政府統計コードをパラメータで受け取る
args = sys.argv
statcode = args[1]
# statcode = "all"

# 日付のフォルダ作成
tbl_dir = root_dir / "table"
tbl_dir.mkdir(parents=True, exist_ok = True)

# テーブルの情報を取得
if statcode == "all":
    statcodes = create_stat_list_of_db(appid)["statcode"].tolist()
else:
    statcodes = [statcode]

for statcode in statcodes:
    table_dest = tbl_dir / f"{statcode}.csv"
    params = {"appId": appid, "statsCode": statcode}
    create_table_list(params).to_csv(table_dest, index = False, quoting=csv.QUOTE_ALL)

