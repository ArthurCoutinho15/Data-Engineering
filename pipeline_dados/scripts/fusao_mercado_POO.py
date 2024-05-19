from processamento_dados import Dados


path_csv = 'C:\\Users\\Arthur Coutinho\\Desktop\\Arthur Coutinho\\Python\\Data_Engineering\\pipeline_dados\\raw_data\\dados_empresaB.csv'
path_json = 'C:\\Users\\Arthur Coutinho\\Desktop\\Arthur Coutinho\\Python\\Data_Engineering\\pipeline_dados\\raw_data\\dados_empresaA.json'

dados_empresa_A = Dados.leitura_dados(path_csv, 'csv')
dados_empresa_B = Dados.leitura_dados(path_json, 'json')


print(dados_empresa_A.dados[0])
print(dados_empresa_B.dados[0])
print('\n ---------------')

print(f'Colunas: {dados_empresa_A.nome_colunas}')
print(f'Colunas: {dados_empresa_B.nome_colunas}')
print('\n ---------------')

#Novos nomes de colunas valor do dicionário
key_mapping = {'Nome do Item': 'Nome do Produto',
               'Classificação do Produto': 'Categoria do Produto',
               'Valor em Reais (R$)': 'Preço do Produto (R$)',
               'Quantidade em Estoque':'Quantidade em Estoque',
               'Nome da Loja':'Filial',
               'Data da Venda':'Data da Venda'}

dados_empresa_A.rename_columns(key_mapping)

print(f'Dados das colunas empresa b {dados_empresa_B.nome_colunas}')
print(f'Dados das colunas empresa a {dados_empresa_A.nome_colunas}')
print('\n ---------------')

print(f'Tamanho dados emrpresa A: {dados_empresa_A.qtd_linhas}')
print(f'Tamanho dados emrpresa B: {dados_empresa_B.qtd_linhas}')
print('\n ---------------')

dados_fusao = Dados.join_data(dados_empresa_A, dados_empresa_B)

print(f'Dados fusão: {dados_fusao.nome_colunas}')
print(f'Dados fusão: {dados_fusao.qtd_linhas}')
print('\n ---------------')


path_dados_combinados = 'processed_data/dados_combinados.csv'
dados_fusao.salvando_dados(path_dados_combinados)

print(f'Caminho Dados fusão: ')

print(dados_empresa_A.nome_colunas)
print(dados_empresa_A.qtd_linhas)

print(dados_empresa_B.nome_colunas)
print(dados_empresa_B.qtd_linhas)

print(dados_fusao.qtd_linhas)

