"""
Classe com o objetivo de criar variaveis ou master table para o modelo
Autor: Wilgner
"""
from typing import List
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import io
import sys

sys.path.append('../funcoes_leitura/')

from funcoes_leitura.pega_arquivo import pega_arquivos_aws
from funcoes_leitura.envia_arquivos import envia_arquivos_aws

class Variaveis:

    def __init__(self, user: str, data_de_execucao: datetime, key_name: str, granularidade=False, tipo_de_transformacao=False, nome_do_arquivo=False) -> None:
        self.user = user
        self.data_de_execucao = data_de_execucao
        self.key_name = key_name
        self.granularidade = granularidade.upper()
        self.transformacao = tipo_de_transformacao
        self.nome_do_arquivo = nome_do_arquivo
        self.tipos_de_granularidade = {
            'ANUAL': 'AS',
            'SEMESTRAL': '6M',
            'TRIMESTRAL': '3M',
            'MENSAL': 'MS',
            'DIARIA': 'D'}

        self.tipos_de_transformacoes

        self.X = None
        self.X_transformado = None

    def le_arquivo(self):


        objeto_boto = pega_arquivos_aws(
            user=self.user,
            pasta_aws='DADOS_DE_DOMINIO',
            key_name=self.key_name
        )

        self.X = pd.read_parquet(io.BytesIO(objeto_boto.read()))

        print('DADOS BAIXADOS COM SUCESSO')

    def filtra_granularidade(self):

        if not isinstance(self.granularidade, bool):
            tipo_de_granularidade = list(filter(lambda x: x == self.granularidade, self.tipos_de_granularidade))

            self.X['DATAS'] = pd.to_datetime(self.X)

            codigo_frequencia = self.tipos_de_granularidade[tipo_de_granularidade]

            lista_datas = pd.date_range(start=self.X['DATAS'].min(), end=self.X['DATAS'].max(), freq=f'{codigo_frequencia}').tolist()
            self.X_transformado = self.X[self.X['DATAS'].isin(lista_datas)].copy()

            return self.X_transformado

        if isinstance(self.granularidade, bool):
            self.X_transformado = self.X.copy()
            return self.X_transformado

    def transforma_dados(self):

        return None

    def cria_variavel(self):

        return None


