import sys
import os
import pandas as pd

sys.path.append('../')

from Infraestrutura.infraestrutura import Infraestrutura
from Infraestrutura.configs import *

def pega_arquivos_aws(user: str, pasta_aws: str, key_name: str) -> pd.DataFrame:


    aws_database = Infraestrutura(
        user=user,
        aws_access_key_id=USER_ACESS[user]['AWS_ACESS_KEY_ID'],
        aws_secret_access_key=USER_ACESS[user]['AWS_SECRET_ACESS_KEY'],
        bucket_name=BUCKET_NAME
    )

    aws_database.loga_no_database()
    aws_object = aws_database.get_object(key_name=f"{pasta_aws}/{key_name}")

    return aws_object
