import io

import pandas as pd
import sys

sys.path.append('../funcoes_leitura')

from funcoes_leitura.pega_arquivo import pega_arquivos_aws

import lightgbm as lgb
from sklearn.model_selection import StratifiedShuffleSplit, GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from boruta import BorutaPy


class Model:

    def __init__(self, user: str, id_tabela_x: str, nome_coluna_y: str, qual_modelo: str, tamanho_do_test: float,
                 random_state: int):

        self.user = user
        self.id_tabela_x = id_tabela_x
        self.coluna_y = nome_coluna_y
        self.nome_modelo = qual_modelo
        self.tamanho_do_test = tamanho_do_test
        self.random_state = random_state

        self.tabela = None
        self.treino = None
        self.teste = None
        self.X_boruta = None
        self.Y = None
        self.X_treino = None
        self.y_treino = None
        self.X_teste = None
        self.y_teste = None

        parametros_default_lightgbm = {
            'type': 'LGBM',
            'params': {
                'boosting_type': 'rf',
                'bagging_fraction': 0.8,
                'bagging_freq': 1,
                'random_state': self.random_state
            }
        }
        self.tipos_de_modelo = {
            'rf': RandomForestClassifier(),
            'lgb': lgb.LGBMClassifier(**parametros_default_lightgbm['params'])
        }

        self.modelo_sem_grid_search = self.tipos_de_modelo[self.nome_modelo]
        self.modelo_com_grid_search = None

    def baixa_base(self):

        objeto = pega_arquivos_aws(
            user=self.user,
            pasta_aws='MODEL_INPUT',
            key_name=self.id_tabela_x
        )

        self.tabela = pd.read_parquet(io.BytesIO(objeto.read()))

    def feature_selection(self):

        feature_selector = BorutaPy(
            self.modelo_sem_grid_search,
            n_estimators=100,
            verbose=0,
            random_state=self.random_state,
            max_iter=10
        )

        self.X_boruta = self.tabela.drop('RISCO DE FOGO', axis=1).values
        self.Y = self.tabela['RISCO DE FOGO'].values

        feature_selector.fit(self.X_boruta, self.Y)

        self.tabela = feature_selector.transform(self.X_boruta)
        self.tabela['RISCO DE FOGO'] = self.Y.values

    def divide_amostra(self):

        split = StratifiedShuffleSplit(
            n_splits=1,
            test_size=self.tamanho_do_test,
            random_state=self.random_state
        )

        for treino_index, test_index in split.splits(self.tabela, self.tabela['RISCO DE FOGO']):
            self.treino = self.tabela.iloc[treino_index, :]
            self.teste = self.tabela.iloc[test_index, :]

    def separa_variaveis(self):

        self.X_treino = self.treino.drop('RISCO DE FOGO', axis=1).values
        self.y_treino = self.treino['RISCO DE FOGO'].values

        self.X_teste = self.teste.drop('RISCO DE FOGO', axis=1).values
        self.y_teste = self.teste['RISCO DE FOGO'].values

    def realiza_grid_search(self):

        if self.nome_modelo == 'rf':
            params = {
                'n_estimators': list(range(1, 1000, 200)),
                'max_depth': list(range(1, 1000, 200)),
                'max_features': ['auto', 'sqrt'],
                'bootstrap': [True, False]
            }

        else:
            # lightGBM
            params = {
                'n_estimators': list(range(1, 1000, 200)),
                'num_leaves': list(range(1, 1000, 200)),
            }

        self.modelo_com_grid_search = GridSearchCV(
            self.modelo_sem_grid_search,
            params
        )

    def treina_modelo(self):

        self.modelo_com_grid_search.fit(self.X_treino, self.y_treino)

    