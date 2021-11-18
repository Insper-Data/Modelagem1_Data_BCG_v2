import io
import time
import pandas as pd
import sys

sys.path.append('../')

from funcoes_leitura.pega_arquivo import pega_arquivos_aws

import lightgbm as lgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import r2_score, roc_curve, auc, classification_report, mean_squared_error
from boruta import BorutaPy
import optuna
import numpy as np


class Model:

    def __init__(self, user: str, id_tabela_x: str, nome_coluna_y: str, qual_modelo: str, tamanho_do_test: float):
        self.user = user
        self.id_tabela_x = id_tabela_x
        self.coluna_y = nome_coluna_y
        self.nome_modelo = qual_modelo
        self.tamanho_do_test = tamanho_do_test

        self.tabela = None
        self.treino = None
        self.teste = None
        self.X_boruta = None
        self.Y = None
        self.X_treino = None
        self.y_treino = None
        self.X_teste = None
        self.y_teste = None
        self.previsão = None

        parametros_default_lightgbm = {
            'type': 'LGBM',
            'params': {
                'boosting_type': 'rf',
                'bagging_fraction': 0.8,
                'bagging_freq': 1,
                'random_state': 201
            }
        }
        self.tipos_de_modelo = {
            'rf': RandomForestClassifier(),
            'lgb': lgb.LGBMClassifier(**parametros_default_lightgbm['params'])
        }

        self.modelo_sem_grid_search = lgb.LGBMClassifier(**parametros_default_lightgbm['params'])
        self.model = None

    def baixa_base(self):
        objeto = pega_arquivos_aws(
            user=self.user,
            pasta_aws='MODEL_INPUT',
            key_name=self.id_tabela_x
        )

        self.tabela = pd.read_parquet(io.BytesIO(objeto.read()))
        print('BASE BAIXADA COM SUCESSO')

    def feature_selection(self):
        feature_selector = BorutaPy(
            self.modelo_sem_grid_search,
            n_estimators=100,
            verbose=0,
            random_state=201,
            max_iter=10
        )
        self.tabela.reset_index(inplace=True)
        datas = self.tabela['Data']
        self.X_boruta = self.tabela.drop(['Data', self.coluna_y], axis=1).values
        self.Y = self.tabela[self.coluna_y].values

        feature_selector.fit(self.X_boruta, self.Y)

        self.tabela = pd.DataFrame(feature_selector.transform(self.X_boruta))
        self.tabela[self.coluna_y] = self.Y
        self.tabela['Data'] = datas
        print('BORUTA EXECUTADO !!')

    def objective(self):
        print(self.tamanho_do_test)
        cv = TimeSeriesSplit(n_splits=3)

        parameters = {'num_leaves': [20, 40, 60, 80, 100], 'min_child_samples': [5, 10, 15],
                      'max_depth': [-1, 5, 10, 20],
                      'learning_rate': [0.05, 0.1, 0.2], 'reg_alpha': [0, 0.01, 0.03]}

        self.model = GridSearchCV(self.modelo_sem_grid_search, parameters, scoring='r2')

        for treino_index, test_index in cv.split(self.tabela, self.tabela[self.coluna_y]):
            self.treino = self.tabela.iloc[treino_index, :]
            self.teste = self.tabela.iloc[test_index, :]

            print('AMOSTRAS DIVIDIDAS')

            self.X_treino = self.treino.drop(self.coluna_y, axis=1).values
            self.y_treino = self.treino[self.coluna_y].values

            self.X_teste = self.teste.drop(self.coluna_y, axis=1).values
            self.y_teste = self.teste[self.coluna_y].values

            print('VARIAVEIS SEPARADAS')

            self.model.fit(X=self.X_treino, y=self.y_treino)

            print(self.model.best_params_)

            self.previsão = self.model.predict(X_teste)


    def testa_modelo(self):

        self.previsão = self.model.predict(X_teste)
        print('PREVISÃO REALIZADA !')

    def metricas_teste(self):
        r2 = r2_score(self.y_teste, self.previsão)
        fpr, tpr, thresholds = roc_curve(self.y_teste, self.previsão)
        auc_metric = auc(fpr, tpr)

        print("******************************")
        print("METRICAS:")
        print('R2 SCORE:')
        print(r2)
        print('AUC:')
        print(auc_metric)

    def executa_modelo(self):
        self.baixa_base()
        self.feature_selection()
        self.objective()
        self.testa_modelo()
        self.metricas_teste()
