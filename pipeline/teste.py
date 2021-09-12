# from model import Model
# import sys
# sys.path.append('../')
from modelpipeline import ModelPipeline

# modelo = Model(
#     user='Wilgner',
#     id_tabela_x='model_input_teste_2021-08-14.parquet',
#     nome_coluna_y='TARGET',
#     qual_modelo='rf',
#     tamanho_do_test=3,
#     random_state=404
# )
#
# modelo.executa_modelo()

modelpipeline_x = ModelPipeline(
    user='Wilgner',
    id_tabela_x='model_input_teste_2021-08-14.parquet',
    coluna_Y='TARGET',
    qual_modelo='rf',
    tamanho_do_test=3,
    random_state=404
)
modelpipeline_x.make_model_pipeline()