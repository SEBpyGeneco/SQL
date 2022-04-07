import pandas as pd
import numpy as np

read_file = pd.read_excel ('/net/cremi/serichard/espaces/travail/sql/Projet/fichiers/DD-indic-reg-dep_2008_2019.xls', sheet_name='Social')
read_file.to_csv ("/net/cremi/serichard/espaces/travail/sql/Projet/fichiers/new_csv.csv")
df = pd.read_csv("/net/cremi/serichard/espaces/travail/sql/Projet/fichiers/new_csv.csv",sep =",")

df = df.dropna()
# print(df)

# Indicateurs - Dimension sociale
# for i in range(len(df.index)):
#     if df.iloc[i]['Indicateurs - Dimension sociale'] in ["P","M","F"]:

df = df.rename(columns = ['Indicateurs','Regions','EsperanceVIE_Homme','EsperanceVIE_Femme','Disparité','Pauvrete','Insertion','>7mn_Sante','Pop_inondable'])

def splitdf_at(df,word,col):
    for i in range(len(df.index)):
        if df.iloc[i][col] == word:

            df_1 = df.iloc[:i,:]
            df_2 = df.iloc[i:,:]
            return df_1,df_2
    return None

df1,df2 = splitdf_at(df,"Ain","Unnamed: 1")
print(df1)
print(df2)
