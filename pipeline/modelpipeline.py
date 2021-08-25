import sys

sys.path.append('../')

from model.model import Model


class ModelPipeline:

    def __init__(self, user: str, id_tabela_x, coluna_Y, qual_modelo, tamanho_do_test, random_state):

        self.user = user
        self.id_tabela_X = id_tabela_x
        self.coluna_Y = coluna_Y
        self.modelo = qual_modelo
        self.tamanho_do_test = tamanho_do_test
        self.random_state = random_state

        self.model = Model(
            user=self.user,
            id_tabela_x=self.id_tabela_X,
            nome_coluna_y=self.coluna_Y,
            qual_modelo=self.modelo,
            tamanho_do_test=self.tamanho_do_test,
            random_state=self.random_state
        )

    def cria_modelo(self):

        self.model.executa_modelo()

    def make_model_pipeline(self):

        self.cria_modelo()
