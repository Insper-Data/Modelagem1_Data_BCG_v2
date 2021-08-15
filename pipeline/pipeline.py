from datetime import date
import sys

sys.path.append('../')

from variaveis.variaveis import Variaveis
from model_input.model_Input import ModelInput
from model.model import Model


class Pipeline:

    def __init__(self, user: str, key_name: str, coluna_para_transformar_X: str, coluna_para_transformar_y,
                 versao: str, tipo: str, qual_modelo: str, tamanho_do_test: str, random_state: int, granularidade=False,
                 transformacao_X=False, transformacao_y=False,
                 nome_do_arquivo=False):
        self.user = user
        self.key_name = key_name
        self.coluna_para_transformar_X = coluna_para_transformar_X
        self.coluna_para_transformar_y = coluna_para_transformar_y
        self.versao = versao
        self.tipo = tipo
        self.granularidade = granularidade.upper()
        self.transformacao_X = transformacao_X
        self.transformacao_y = transformacao_y
        self.nome_do_arquivo = nome_do_arquivo
        self.variaveis_X = Variaveis(self.user, date.today(), self.key_name, self.coluna_para_transformar_X,
                                     self.versao,
                                     'X', self.tipo, self.granularidade, self.transformacao_X, self.nome_do_arquivo)

        self.variaveis_y = Variaveis(self.user, date.today(), self.key_name, self.coluna_para_transformar_y,
                                     self.versao,
                                     'Y', self.tipo, self.granularidade, self.transformacao_y, self.nome_do_arquivo)

        self.id_tabela_X = f'{self.versao}_{self.tipo}_X_{self.granularidade}_{date.today()}.parquet'

        self.id_tabela_y = f'{self.versao}_{self.tipo}_Y_{self.granularidade}_{date.today()}.parquet'

        self.model_input = ModelInput(self.user, self.id_tabela_X, self.id_tabela_y, self.tipo, self.granularidade)

        self.model = Model(
            user=self.user,
            id_tabela_x=self.id_tabela_X,
            nome_coluna_y=self.coluna_para_transformar_y,
            qual_modelo=qual_modelo,
            tamanho_do_test=tamanho_do_test,
            random_state=random_state
        )

    # funcao que transforma os dados de dominio em variaveis explicativas
    def cria_variaveis_X(self):
        self.variaveis_X.cria_variavel()
        print('VARIAVEIS X CRIADAS COM SUCESSO')

    # funcao que transforma os dados de dominio em variaveis alvo
    def cria_variaveis_y(self):
        self.variaveis_y.cria_variavel()
        print('VARIAVEIS Y CRIADAS COM SUCESSO')

    # funcao que transforma as variaveis em model input
    def cria_model_input(self):
        self.model_input.cria_tabela_de_variaveis()
        print('MODEL INPUT CRIADO COM SUCESSO')

    def cria_modelo(self):
        self.model.executa_modelo()

    # funcao responsavel por rodar o pipeline
    def make_pipeline(self):
        self.cria_variaveis_X()
        self.cria_variaveis_y()
        self.cria_model_input()
        self.cria_modelo()
