
import pandas as pd
import requests
import time

def to_list(obj):
    obj = obj if isinstance(obj, list) else [obj]
    return(obj)



def class_to_df(class_obj):
    return(pd.json_normalize(to_list(class_obj["CLASS"])
                      ).astype(str
                      ).assign(id = class_obj["@id"]
                      ).assign(meta_name = class_obj["@name"]
                    ))


def create_table_list(params):

    url = f"http://api.e-stat.go.jp/rest/3.0/app/json/getStatsList"

    res = requests.get(url, params = params)

    status = res.json()["GET_STATS_LIST"]["RESULT"]["STATUS"]

    if status == 1:
        return(None)
    else:
        table_infs = to_list(res.json()["GET_STATS_LIST"]["DATALIST_INF"]["TABLE_INF"])
        return(pd.concat([pd.json_normalize(table_inf).astype(str) for table_inf in table_infs]))


def create_stat_list_of_db(appid):

    url = f"http://api.e-stat.go.jp/rest/3.0/app/json/getStatsList?appId={appid}&statsNameList=Y"

    res = requests.get(url)

    data = res.json()["GET_STATS_LIST"]["DATALIST_INF"]["LIST_INF"]

    renames = {
        'STAT_NAME.@code': 'statcode',
        'STAT_NAME.$': 'statname',
        'GOV_ORG.@code': 'govcode',
        'GOV_ORG.$' : 'govname',
        }

    return(pd.json_normalize(data).rename(columns= renames))


def create_meta_list(appid: str, statdisp_id: str, retry_count: int = 5):

        retry = 1
        state = 504
        while retry <= retry_count and state == 504: 
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

        return(pd.concat([class_to_df(class_obj) for class_obj in class_objs if "CLASS" in class_obj]))
