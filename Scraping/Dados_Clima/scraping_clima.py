import sys
sys.path.append('C:/Users/rapha/OneDrive - Insper - Institudo de Ensino e Pesquisa/Insper/Insper Data/Projeto Modelagem 21.2/Modelagem1_Data_BCG_v2/funcoes_leitura')
sys.path.append('C:/Users/rapha/OneDrive - Insper - Institudo de Ensino e Pesquisa/Insper/Insper Data/Projeto Modelagem 21.2/Modelagem1_Data_BCG_v2/Infraestrutura')
import pandas as pd
from executa import df
import io
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from tqdm import tqdm
import shutil 
import os
import time

queimadas = df

driver = webdriver.Chrome()

driver.get('https://climateknowledgeportal.worldbank.org/download-data')

variables = Select(driver.find_element_by_id('variablesHist'))

variaveis_temperaturas = {
    'Mean-Temperature': ['tas', 'Temperature - (Celsius)'],
    'Max-Temperature': ['tasmax', 'tasmax'],
    'Min-Temperature': ['tasmin', 'tasmin']
}

clima_df_final = pd.DataFrame()

for index, row in tqdm(queimadas.iterrows(), total = queimadas.shape[0], desc = 'Extraindo dados...'):
        
    primeira_variavel = True
    clima_df = pd.DataFrame(columns = ['Data', 'Municipio', 'Longitude', 'Latitude'])

    for variavel_temperatura in variaveis_temperaturas:
        variables.select_by_visible_text(variavel_temperatura)

        latitude = driver.find_element_by_id('latitude')
        latitude.clear()
        latitude.send_keys(str(row.latitude))

        longitude = driver.find_element_by_id('longitude')
        longitude.clear()
        longitude.send_keys(str(row.longitude))

        download = driver.find_element_by_id('downloadHistoricalBtn')
        download.click()

        path = 'C:/Users/rapha/Downloads/' + variaveis_temperaturas[variavel_temperatura][0] + '_1991_2020_' + str(row.longitude) + '_' + str(row.latitude) + '.csv'

        while not os.path.exists(path):
            time.sleep(0.1)

        dados = pd.read_csv(path)
        dados.rename(columns={variaveis_temperaturas[variavel_temperatura][1]: variavel_temperatura, ' Longitude': 'Longitude', ' Latitude': 'Latitude'}, inplace=True)
        dados[' Statistics'] = dados[' Statistics'].str.replace('Average', '')
        dados['Data'] = dados[' Statistics'] + '/' + dados[' Year'].astype(str)
        dados['Municipio'] = row.municipio
        dados.drop(columns=[' Statistics', ' Year'], inplace=True)

        deleted = False
        while not deleted:
            if os.path.exists(path):
                os.remove(path)
                deleted = True   

        if primeira_variavel:
            clima_df = clima_df.append(dados)
            clima_df.reset_index(drop=True)

            primeira_variavel = False
            continue

        clima_df = clima_df.merge(dados, on=['Data', 'Municipio', 'Longitude', 'Latitude'], how='inner', validate = 'one_to_one')
        clima_df.reset_index(drop=True, inplace = True)

    clima_df_final = clima_df_final.append(clima_df)

driver.close()

clima_df_final.reset_index(drop=True, inplace=True)
clima_df_final.to_parquet('WORLDBANK_CLIMA_MUNICIPIOS_MENSAL.parquet', index=False)