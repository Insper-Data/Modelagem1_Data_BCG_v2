"""
Classe com o objetivo de criar variaveis ou master table para o modelo
Autor: Wilgner
"""
import os
from datetime import datetime, date
import pandas as pd
import pickle
import io
import sys

sys.path.append('../')

from funcoes_leitura.pega_arquivo import pega_arquivos_aws
from funcoes_leitura.envia_arquivos import envia_arquivos_aws


class Variaveis:

    def __init__(self, user: str, data_de_execucao: datetime, key_name: str, coluna_para_transformar: str, versao: str,
                 x_ou_y: str, tipo: str, granularidade=False, transformacao=False , nome_do_arquivo=False) -> None:
        self.user = user
        self.data_de_execucao = data_de_execucao
        self.key_name = key_name
        self.granularidade = granularidade.upper()
        self.transformacao = transformacao
        self.coluna_transformar = coluna_para_transformar
        self.nome_do_arquivo = nome_do_arquivo
        self.versao_da_variavel = versao
        self.X_ou_Y = x_ou_y.upper()
        self.tipo = tipo
        self.tipos_de_granularidade = {
            'ANUAL': 'AS',
            'SEMESTRAL': '6M',
            'TRIMESTRAL': '3M',
            'MENSAL': 'MS',
            'DIARIA': 'D'}


        self.tipos_de_transformacoes = {
            'MEDIA': 'transformado.mean()',
            'DESVIO PADRAO': 'transformado.std()',
            'MEDIANA': 'transformado.median()'
        }

        self.X = None
        self.X_transformado = None
        self.variavel = None
        self.documentacao = None
        self.file_name = f'{self.tipo}.parquet'
        self.doc_name = f'documentacao_{self.tipo}_{date.today()}.pickle'

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

            self.X['DATAS'] = pd.to_datetime(self.X['DATAS'], infer_datetime_format=True)

            codigo_frequencia = self.tipos_de_granularidade[tipo_de_granularidade[0]]

            lista_datas = pd.date_range(start=self.X['DATAS'].min(), end=self.X['DATAS'].max(),
                                        freq=f'{codigo_frequencia}').tolist()

            self.X_transformado = self.X[self.X['DATAS'].isin(lista_datas)].copy()

        if isinstance(self.granularidade, bool):
            self.X_transformado = self.X.copy()

    def transforma_dados(self):

        if isinstance(self.coluna_transformar, list):
            transformado = eval(f"self.X_transformado.groupby(by='DATAS'){self.coluna_transformar}")

            resultado = eval(self.tipos_de_transformacoes[self.transformacao])

        else:
            transformado = self.X_transformado.groupby(by='DATAS')[self.coluna_transformar]

            resultado = eval(self.tipos_de_transformacoes[self.transformacao])

        return resultado

    def envia_arquivo_e_documentacao(self):
        envia_arquivos_aws(
            path='.',
            user=self.user,
            nome_do_arquivo=self.file_name,
            pasta_aws='VARIAVEIS',
            key_name=f'{self.versao_da_variavel}_{self.tipo}_{self.X_ou_Y}_{self.granularidade}_{date.today()}.parquet'
        )
        print('ARQUIVO ENVIADO COM SUCESSO')

        envia_arquivos_aws(
            path='.',
            user=self.user,
            nome_do_arquivo=self.doc_name,
            pasta_aws='VARIAVEIS/documentacao',
            key_name=f'{self.versao_da_variavel}_documentacao_{self.tipo}_{self.X_ou_Y}_{self.granularidade}_{date.today()}.pickle'
        )
        print('ARQUIVO ENVIADO COM SUCESSO')

    def cria_variavel(self):

        parquet_path_name = f'./{self.file_name}'
        txt_path_name = f'./{self.doc_name}'

        self.le_arquivo()
        self.filtra_granularidade()
        self.variavel = self.transforma_dados()


        if isinstance(self.variavel, pd.core.series.Series):
            df = pd.DataFrame(self.variavel)
            df.to_parquet(parquet_path_name)

        else:
            self.variavel.to_parquet(parquet_path_name)

        self.documentacao = {
            'usuario': self.user,
            'data_de_criacao': date.today(),
            'dado_de_dominio_utilizado': self.key_name,
            'granularidade_usada': self.granularidade,
            'tranformacao_usada': self.transformacao,
        }

        file = open(txt_path_name, "wb")
        pickle.dump(self.documentacao, file)

        self.envia_arquivo_e_documentacao()

        print('VARIAVEL CRIADA COM SUCESSO')

        return self.variavel
