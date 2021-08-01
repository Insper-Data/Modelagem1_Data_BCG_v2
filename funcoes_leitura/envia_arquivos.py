import sys
import os

sys.path.append('../Infraestrutura/')

from infraestrutura import Infraestrutura
from configs import *

print(os.listdir(os.getcwd()))
def envia_arquivos_aws(path: str, user: str, nome_do_arquivo: str, pasta_aws: str, key_name: str) -> None:


    aws_database = Infraestrutura(
        user=user,
        aws_access_key_id=USER_ACESS[user]['AWS_ACESS_KEY_ID'],
        aws_secret_access_key=USER_ACESS[user]['AWS_SECRET_ACESS_KEY'],
        bucket_name=BUCKET_NAME
    )

    aws_database.loga_no_database()
    aws_database.upload_object(file=f"{path}\\{nome_do_arquivo}.parquet", key_name=f"{pasta_aws}/{key_name}")
    print(aws_database.lista_objects())