import requests 
import pandas as pd
from math import ceil

class Data():
    def __init__(self, owner):
        self.owner = owner
        self.api_base_url = 'https://api.github.com'
        self.access_token = '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'
        self.headers = {'Authorization':'Bearer ' + self.access_token,
                        'X-GitHub-Api-Version':'2022-11-28'}
         

    def lista_repositorios(self):
        repos_list = []
        
        response = requests.get(f'https://api.github.com/users/{self.owner}')
        num_pages = ceil(response.json()['public_repos']/30)
        
        for page_num in range(1,num_pages):
            try:
                url = f'{self.api_base_url}/users/{self.owner}/repos?page={page_num}'
                response = requests.get(url, headers=self.headers)
                repos_list.append(response.json())
            except:
                repos_list.append(None)
        return repos_list
    
    def nomes_repos(self,repos_list):
        repos_names = []
        
        for page in repos_list:
            for repo in page:
                try:
                    repos_names.append(repo['name'])
                except:
                    pass
        
        return repos_names

    def nome_linguagens(self, repos_list):
        repos_linguagens = []
        
        for page in repos_list:
           for repo in page:
               try:
                repos_linguagens.append(repo['language'])
               except:
                   pass
        return repos_linguagens
    
    def cria_df_linguagens(self):
        repositorios = self.lista_repositorios()
        nomes = self.nomes_repos(repositorios)
        linguagens = self.nome_linguagens(repositorios)
        
        dados = pd.DataFrame()
        dados['repository_name'] = nomes
        dados['language'] = linguagens
        
        dados.to_csv(f'C:\\Users\\Arthur Coutinho\\Desktop\\Arthur Coutinho\\Python\\Data_Engineering\\Projeto_requests\\processed_data\\{self.owner}.csv', index=False)
        print('Csv gerado com sucesso')
        
        return dados
    
    