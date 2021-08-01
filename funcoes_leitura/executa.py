import os

from envia_arquivos import envia_arquivos_aws
from path_wil import PATH_LOCAL_BASE
from datetime import date
from pega_arquivo import pega_arquivos_aws
from tqdm import tqdm
import pandas as pd
import io

user = 'Wilgner'
commodity_list = ['Boi_Gordo_Futuros_Dados_Históricos',
 'Café_Contrato_C_Futuros_Dados_Históricos',
 'Madeira_Serrada_Futuros_Dados_Históricos',
 'Soja_NY_Futuros_Dados_Históricos',
 'Suco_de_Laranja_NY_Futuros_Dados_Históricos']


envia_arquivos_aws(
    path=PATH_LOCAL_BASE,
    user=user,
    nome_do_arquivo='clima_20_anos_brasil',
    pasta_aws='RAW/clima',
    key_name=f'clima_20_anos_brasil_{date.today()}.parquet'
)

# object_boto = pega_arquivos_aws(
#     user=user,
#     pasta_aws='RAW',
#     key_name='ibge_populacao_municipio_2021-08-01.parquet'
# )
# print(object_boto)
# df = pd.read_parquet(io.BytesIO(object_boto.read()))
# print(df.head())