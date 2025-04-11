import pandas as pd 
import os
import glob

# função que e extrai, ler e consolida os json
def concatena_e_le_arquivos(pasta: str) -> pd.DataFrame:
    arquivo_json =  glob.glob(os.path.join(pasta, '*json'))  
    lista_arquivos_json = [pd.read_json(arquivo) for arquivo in arquivo_json]
    df_final = pd.concat(lista_arquivos_json, ignore_index=True)
    return df_final


# função que tranforma faturamento por produto
def calcula_faturamento_por_produto(df: pd.DataFrame) -> pd.DataFrame:
    df["Faturamento_produto"] = df["Quantidade"] * df["Venda"]
    return df


#funcao que calcula o lucro por produto
def calculo_lucro_por_produto(df: pd.DataFrame) -> pd.DataFrame: 
    df["Lucro "] = (df["Venda"] - df["Custo"]) * df["Quantidade"]
    return df

#função que  da load em csv ou parquet
def carregar_dados(df: pd.DataFrame, formatos: list):

    for formato in formatos:
        if formato == 'csv':
            df.to_csv("dados.csv", index=False)
        elif formato == 'parquet':
            df.to_parquet( "dados.parquet", index=False)

def pipeline(pasta_entrada: str, formatos_saida: list):
    dados = concatena_e_le_arquivos(pasta_entrada)
    
    # Aplica os dois cálculos no mesmo DataFrame
    dados = calcula_faturamento_por_produto(dados)
    dados = calculo_lucro_por_produto(dados)

    # Exporta o DataFrame final
    carregar_dados(dados, formatos_saida)
    
    # (Opcional) Retorna o DataFrame final para uso
    return dados


