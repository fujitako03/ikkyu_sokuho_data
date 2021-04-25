import datetime

import numpy as np
import pandas as pd
import pandas_gbq
from omegaconf import OmegaConf


def load_to_bigquery(df, project_id, db_name, table_name, if_exists="fail", schema_dict=None):
    table_id = db_name + "." + table_name

    print(f"load to {table_id}")

    # アップロード
    pandas_gbq.to_gbq(
        df, 
        destination_table=table_id, 
        project_id=project_id,
        if_exists=if_exists,
        table_schema=schema_dict,
    )
    print("success")

    return None
