import boto3
import pandas as pd
from typing import Dict, List


class Infraestrutura:
    """
    Classe destinada a criar e gerenciar infraestrutura AWS S3
    Parametros necessarios:
    USER: Usuario
    AWS_ACESS_KEY: Codigo de acesso da AWS
    AWS_SECRET_KEY_ACESS: Codigo de acesso da AWS
    BUCKET_NAME: Nome do bucket na AWS
    """

    def __init__(self,
                 user: str,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 bucket_name: str):
        self.user = user.capitalize()
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._bucket_name = bucket_name
        self.s3 = None

    def loga_no_database(self) -> None:
        self.s3 = boto3.client('s3',
                               region_name='us-east-1',
                               aws_access_key_id=self._aws_access_key_id,
                               aws_secret_access_key=self._aws_secret_access_key)

    def lista_buckets(self) -> List:
        response = self.s3.list_buckets()
        return response['Buckets']

    def cria_bucket(self, bucket_name: str) -> None:
        self.s3.create_bucket(Bucket=bucket_name)
        print(f'O bucket {bucket_name} foi criado !!')

    def deleta_bucket(self, bucket_name: str) -> None:
        self.s3.delete_bucket(Bucket=bucket_name)
        print(f'O bucket {bucket_name} foi deletado !!')

    def upload_object(self, file: str, key_name: str) -> None:
        self.s3.upload_file(Filename=file,
                            Bucket=self._bucket_name,
                            Key=key_name)

    def lista_objects(self) -> Dict:
        response = self.s3.list_objects(Bucket=self._bucket_name)
        return response['Contents']

    def get_object(self, key_name: str) -> pd.DataFrame:
        response = self.s3.get_object(Bucket=self._bucket_name,
                                    Key=key_name)

        return response['Body']

    def deleta_object(self, key_name: str) -> None:
        self.s3.delete_object(Bucket=self._bucket_name,
                              Key=key_name)

        print(f'O arquivo {key_name} foi deletado com sucesso')

