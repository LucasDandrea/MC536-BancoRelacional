import pandas as pd

# Carregar os arquivos
localizacao_path = '../dados/tratados/localizacao_com_id.csv'
remuneracao_path = '../dados/tratados/remuneracao_com_id.csv'
vinculo_path = '../dados/tratados/vinculo.csv'

# Ler os DataFrames
df_localizacao = pd.read_csv(localizacao_path)
df_remuneracao = pd.read_csv(remuneracao_path)
df_vinculo = pd.read_csv(vinculo_path)

# Atualizar ou adicionar as colunas
df_vinculo['id_loc'] = df_localizacao['id_loc']  # Sobrescreve se já existir
# Sobrescreve se já existir
df_vinculo['id_remuneracao'] = df_remuneracao['id_remuneracao']

# Salvar o arquivo atualizado (mantendo todas as colunas)
df_vinculo.to_csv(vinculo_path, index=False)

print("Colunas atualizadas/adicionadas com sucesso!")
