import json
import csv 

# Funções 
def leitura_json(path_json):
    dados_json = []
    with open(path_json, 'r') as file:
        dados_json = json.load(file)
    return dados_json

def leitura_csv(path_csv ):
    dados_csv = []
    with open(path_csv, 'r', encoding='utf-8') as file:
        spamreader = csv.DictReader(file, delimiter=',')
        for row in spamreader:
            dados_csv.append(row)
    return dados_csv

def leitura_dados(path, tipo_arquivo):
    dados = []
    if(tipo_arquivo == 'csv'):
        dados = leitura_csv(path)
    elif(tipo_arquivo == 'json'):
        dados = leitura_json(path)
    
    return dados

def get_columns(dados):
    return list(dados[0].keys())

def rename_columns(dados, key_mapping):
    new_dados_csv = []
    for old_dict in dados:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_dados_csv.append(dict_temp)
    return new_dados_csv

def size_data(dados):
   return len(dados) 
    
def join_data(dados_A, dados_B):
    combined_list = []
    combined_list.extend(dados_A)
    combined_list.extend(dados_B)
    return combined_list
    
def transformando_dados_tabela(dados, nome_colunas):
    dados_combinados_tabela = [nome_colunas]
    for row in dados:
        linha = []
        for coluna in nome_colunas:
            linha.append(row.get(coluna, 'Indisponível'))
        dados_combinados_tabela.append(linha)
    
    return dados_combinados_tabela      

def salvando_dados(dados, path):
    with open(path, 'w', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(dados) 

path_csv = 'C:\\Users\\Arthur Coutinho\\Desktop\\Arthur Coutinho\\Python\\Data_Engineering\\pipeline_dados\\raw_data\\dados_empresaB.csv'
path_json = 'C:\\Users\\Arthur Coutinho\\Desktop\\Arthur Coutinho\\Python\\Data_Engineering\\pipeline_dados\\raw_data\\dados_empresaA.json'


# Iniciando a leitura dos dados
dados_json = leitura_dados(path_json, 'json')
nome_colunas_json = get_columns(dados_json)

dados_csv = leitura_dados(path_csv, 'csv')
nome_colunas_csv = get_columns(dados_csv)


print(f'Tamanho csv: {size_data(dados_csv)}')
print(f'Tamanho csv: {size_data(dados_json)}')

print(f'JSON: {nome_colunas_json}')
print(f'CSV: {nome_colunas_csv}')

# Transformação dos dados

key_mapping = {'Nome do Item': 'Nome do Produto',
               'Classificação do Produto': 'Categoria do Produto',
               'Valor em Reais (R$)': 'Preço do Produto (R$)',
               'Quantidade em Estoque':'Quantidade em Estoque',
               'Nome da Loja':'Filial',
               'Data da Venda':'Data da Venda'}

dados_csv = rename_columns(dados_csv, key_mapping)

print(f'Novas colunas CSV: {get_columns(dados_csv)}')

dados_fusao = join_data(dados_csv,dados_json )

nomes_colunas_fusao = get_columns(dados_fusao)
print(f'Colunas fusão: {nomes_colunas_fusao}')
print(dados_fusao[0])

tamanho_dados_fusao = size_data(dados_fusao)
print(f'Colunas fusão: {tamanho_dados_fusao}')

# Salvando os dados

dados_fusao_tabela = transformando_dados_tabela(dados_fusao, nomes_colunas_fusao)
print(f'Colunas fusão: {tamanho_dados_fusao}')
print(dados_fusao_tabela[1])

path_dados_fusao = 'processed_data/dados_combinados.csv'

salvando_dados(dados_fusao_tabela, path_dados_fusao)
print(f'Caminho dados salvos: {path_dados_fusao}')

