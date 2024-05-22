from data_processing import Data
from math import ceil

import requests

owner = 'amzn'
url = f'https://api.github.com/users/{owner}'

'''
amazon_rep = Data('amzn')
linguagens_amazon = amazon_rep.cria_df_linguagens()
print(linguagens_amazon)

netflix_rep = Data('netflix')
linguagens_netflix = netflix_rep.cria_df_linguagens()
print(linguagens_netflix)

spotify_rep = Data('spotify')
linguagens_spotify = spotify_rep.cria_df_linguagens()
print(linguagens_spotify)

arthur = Data('ArthurCoutinho15')
arthur_A = arthur.cria_df_linguagens()
print(arthur_A)
'''
apple_rep = Data('apple')
linguagens_apple = apple_rep.cria_df_linguagens()
print(linguagens_apple)