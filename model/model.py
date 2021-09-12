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
        self.previsão = None

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

        self.modelo_sem_grid_search = lgb.LGBMClassifier(**parametros_default_lightgbm['params'])
        self.modelo_tunning = None
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
            random_state=self.random_state,
            max_iter=10
        )
        self.tabela.reset_index(inplace=True)
        datas = self.tabela['DATAS']
        self.X_boruta = self.tabela.drop(['DATAS', self.coluna_y], axis=1).values
        self.Y = self.tabela[self.coluna_y].values

        feature_selector.fit(self.X_boruta, self.Y)

        self.tabela = pd.DataFrame(feature_selector.transform(self.X_boruta))
        self.tabela[self.coluna_y] = self.Y
        self.tabela['DATAS'] = datas
        print('BORUTA EXECUTADO !!')

    def objective(self, trial):
        print(self.tamanho_do_test)
        cv = TimeSeriesSplit(n_splits=5)
        rmse = []

        params = {
            'objective': 'binary',
            'metric': 'rmse',
            'boosting': 'rf',
            'num_leaves': trial.suggest_int(name='num_leaves', low=2, high=200),
            'feature_fraction': trial.suggest_uniform(name='feature_fraction', low=0.4, high=1.0),
            'bagging_fraction': trial.suggest_uniform(name='bagging_fraction', low=0.4, high=1.0),
            'bagging_freq': trial.suggest_int(name='bagging_freq', low=1, high=7),
            'min_child_samples': trial.suggest_int(name='min_child_samples', low=5, high=100),
            'max_depth': trial.suggest_int(name='max_depth', low=5, high=15),
            'learning_rate': trial.suggest_uniform(name='learning_rate', low=0.01, high=0.10)}


        for treino_index, test_index in cv.split(self.tabela, self.tabela[self.coluna_y]):
            self.treino = self.tabela.iloc[treino_index, :]
            self.teste = self.tabela.iloc[test_index, :]

            print('AMOSTRAS DIVIDIDAS')


            self.X_treino = self.treino.drop(self.coluna_y, axis=1).values
            self.y_treino = self.treino[self.coluna_y].values

            self.X_teste = self.teste.drop(self.coluna_y, axis=1).values
            self.y_teste = self.teste[self.coluna_y].values

            print('VARIAVEIS SEPARADAS')


            self.modelo_tunning = lgb.Dataset(self.X_treino, label=self.y_treino)
            self.model = lgb.train(params, self.modelo_tunning)
            previsão = self.model.predict(self.X_teste)

            rmse.append(np.sqrt(mean_squared_error(previsão, self.y_teste)))
        # print(rmse)
        return np.average(rmse)


    def testa_modelo(self):

        study = optuna.create_study(direction="minimize")
        study.optimize(self.objective, n_trials=80)
        
        print("Number of finished trials:", len(study.trials))
        print("Best trial:", study.best_trial.params)
        best_params = study.best_trial.params

        fixed_params = {'objective': 'binary',
                        'metric': 'rmse',
                        'boosting': 'rf',
                        'num_threads': 4}

        best_params = {**best_params, **fixed_params}

        lgb_train = lgb.Dataset(self.X_treino, self.y_treino)
        lgb_eval = lgb.Dataset(self.X_teste, self.y_teste, reference=lgb_train)

        lgbm_model = lgb.train(best_params, lgb_train, 250, valid_sets=lgb_eval,
                                early_stopping_rounds=50, verbose_eval=20)

        self.previsão = lgbm_model.predict(self.X_teste, num_iteration=lgbm_model.best_iteration)

        print('PREVISÃO REALIZADA !')

    def metricas_teste(self):
        # report = classification_report(
        #     self.y_teste,
        #     self.previsão,
        # )
        r2_rf_boruta_test = r2_score(self.y_teste, self.previsão)
        fpr, tpr, thresholds = roc_curve(self.y_teste, self.previsão, pos_label=2)
        auc_metric = auc(fpr, tpr)

        print("******************************")
        print("METRICAS:")
        # print('Classification report')
        # print(report)
        print('R2 SCORE:')
        print(r2_rf_boruta_test)
        print('AUC:')
        print(auc_metric)

    def executa_modelo(self):
        self.baixa_base()
        self.feature_selection()
        self.testa_modelo()
        self.metricas_teste()
