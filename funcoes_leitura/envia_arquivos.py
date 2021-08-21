import sys
import os

sys.path.append('../')

from Infraestrutura.infraestrutura import Infraestrutura
from Infraestrutura.configs import *


def envia_arquivos_aws(path: str, user: str, nome_do_arquivo: str, pasta_aws: str, key_name: str) -> None:


    aws_database = Infraestrutura(
        user=user,
        aws_access_key_id=USER_ACESS[user]['AWS_ACESS_KEY_ID'],
        aws_secret_access_key=USER_ACESS[user]['AWS_SECRET_ACESS_KEY'],
        bucket_name=BUCKET_NAME
    )

    aws_database.loga_no_database()
    aws_database.upload_object(file=f"{path}\\{nome_do_arquivo}", key_name=f"{pasta_aws}/{key_name}")
    # for info in aws_database.lista_objects():
    #     print(info['Key'])
    print('ARQUIVO ENVIADO COM SUCESSO !')
