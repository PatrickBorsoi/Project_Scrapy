import pandas as pd
import sqlite3
from datetime import datetime

df = pd.read_json('../data/data.jsonl', lines=True)
# Setar o pandas para mostrar todas as colunas
pd.options.display.max_columns = None

#Adicionar a coluna _source com um valor fixo
df['_source'] = 'https://lista.mercadolivre.com.br/tenis-corrida-masculinos'
#Adicionar a coluna _data_coleta com a data e hora atual
df['_data_coleta'] = datetime.now()

# Tratar  os valores nulos para colunnas numéricas e de texto
df['old_price_reais'] = df['old_price_reais'].fillna(0).astype(float)
df['old_price_centavos'] = df['old_price_centavos'].fillna(0).astype(float)
df['new_price_reais'] = df['new_price_reais'].fillna(0).astype(float)
df['new_price_centavos'] = df['new_price_centavos'].fillna(0).astype(float)
df['reviews_ratig_number'] = df['reviews_rating_number'].fillna(0).astype(float)

# Remover os parênteses das colunas `reviews_amount`
df['reviews_amount'] = df['reviews_amount'].str.replace('[\(\)]', '', regex=True)
df['reviews_amount'] = df['reviews_amount'].fillna(0).astype(int)

# Tratar os preços como floats e calcular os valores totais
df['old_price'] = df['old_price_reais'] + df['old_price_centavos'] / 100
df['new_price'] = df['new_price_reais'] + df['new_price_centavos'] / 100

# Remover as colunas atigas de preços
df = df.drop(columns=['old_price_reais', 'old_price_centavos', 'new_price_reais', 'new_price_centavos'])

# Conectar ao baco de dados SQLite
conn = sqlite3.connect('../data/quotes.db')

# Salvar o DataFrame no banco de dados SQLite
df.to_sql('Mercadolivre_items', conn, if_exists='replace', index=False)


# Fechar a conexão com o banco de dados
conn.close()

print(df.head())