
import pandas as pd
import requests

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

    table_infs = to_list(res.json()["GET_STATS_LIST"]["DATALIST_INF"]["TABLE_INF"])

    return(pd.concat([pd.json_normalize(table_inf).astype(str) for table_inf in table_infs]))
