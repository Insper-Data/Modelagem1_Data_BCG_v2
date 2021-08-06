import io

import pandas as pd

import sys

sys.path.append('../funcoes_leitura')

from datetime import date
from funcoes_leitura.pega_arquivo import pega_arquivos_aws
from funcoes_leitura.envia_arquivos import envia_arquivos_aws


class ModelInput:

    def __init__(self, user: str, id_tabela_X: str, id_tabela_y: str, tipo_de_variavel: str, granularidades: str):

        self.user = user
        self.id_tabela_X = id_tabela_X
        self.id_tabela_y = id_tabela_y
        self.tipo_de_variavel = tipo_de_variavel
        self.granularidade = granularidades

        self.tabela_X = None
        self.tabela_Y = None
        self.tabela = None
        self.id = None
        self.tabela_name = f'{self.id}.parquet'


    def pega_tabelas(self):

        X = pega_arquivos_aws(
            user=self.user,
            pasta_aws='VARIAVEIS',
            key_name=self.id_tabela_X
        )

        self.tabela_X = pd.read_parquet(io.BytesIO(X.read()))

        y = pega_arquivos_aws(
            user=self.user,
            pasta_aws='VARIAVEIS',
            key_name=self.id_tabela_y
        )

        self.tabela_Y = pd.read_parquet(io.BytesIO(y.read()))

    def envia_model_input(self):

        envia_arquivos_aws(
            path='.',
            user=self.user,
            nome_do_arquivo=self.tabela_name,
            pasta_aws='MODEL_INPUT',
            key_name=self.tabela_name

        )



    def cria_tabela_de_variaveis(self):


        self.pega_tabelas()

        self.tabela_X.rename(columns={'RISCO DE FOGO': 'B'}, inplace=True)

        self.tabela = pd.concat([
            self.tabela_X, self.tabela_Y
        ], axis=1)

        self.id = f'model_input_{self.tipo_de_variavel}_{date.today()}'
        self.tabela_name = f'{self.id}.parquet'

        self.tabela.to_parquet(f'./{self.tabela_name}')

        self.envia_model_input()
        print('ARQUIVO ENVIADO COM SUCESSO')




