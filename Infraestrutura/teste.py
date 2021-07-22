from infraestrutura import Infraestrutura
from configs import *
import pandas as pd

user = 'Wilgner'

teste = Infraestrutura(
    user=user,
    aws_access_key_id=USER_ACESS[user]['AWS_ACESS_KEY_ID'],
    aws_secret_access_key=USER_ACESS[user]['AWS_SECRET_ACESS_KEY'],
    bucket_name='teste-bucket-data-2021'
)

teste.loga_no_database()
print(teste.lista_buckets())
# teste.cria_bucket(bucket_name='teste-bucket-data-2021')
# print(teste.lista_buckets())
# teste.deleta_bucket(bucket_name='teste-bucket-data-2021')
# print(teste.lista_buckets())
# teste.upload_object(file='../aluguel.csv', key_name='aluguel.csv')
# print(teste.lista_objects())
# boto_object = teste.get_object(key_name='aluguel.csv')
# df = pd.read_csv(boto_object, sep=';')
# print(df.head())
# teste.deleta_object(key_name='aluguel.csv')

