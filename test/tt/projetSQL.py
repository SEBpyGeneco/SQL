##### IMPORTATION #####
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras

##### FONCTIONS #####
def CreateTable(nom,attributs):
    cur.execute(f"CREATE TABLE IF NOT EXISTS public.{nom}({attributs}); ")

def InsertInto(nom,attributs,list_values):
    val = ""
    for i in range(len(list_values[0])):
        val += "%s,"
    val = val[:-1]
    
    command = f"INSERT INTO {nom} ({attributs}) VALUES ({val})"
    for value in list_values:
        try:
            cur.execute(command, value)
        except Exception as e :
            cur.close()
            conn.close()
            exit("error when running: " + command + " : " + str(e))

def CSV_typeCorrection(df):
    rows=[]
    for i in range (len(df.index)):
        new_row = list(df.iloc[i])
        for i in range(len(new_row)):
            if isinstance(new_row[i], float):
                new_row[i] = str(new_row[i])
            elif isinstance(new_row[i], np.generic):
               new_row[i] = int(new_row[i])
        rows.append(new_row)
    return rows


##### MAIN #####

#Connexion à la base de données
print("Connexion à la base de donnees...")
USERNAME="admin"
PASSWORD="admin"
try:
    conn = psycopg2.connect(host="pgsql", dbname=USERNAME,user=USERNAME,password=PASSWORD)
except Exception as e :
    exit("Connexion impossible à la base de donnees..." + str(e))
print("Connecté à la base de donnees...")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# #Création des tables
CreateTable("Regions","ID_reg SMALLINT,libelle_reg TEXT NOT NULL, PRIMARY KEY (ID_reg)")
CreateTable("Departements","ID_dep VARCHAR(3) NOT NULL, ID_reg SMALLINT, libelle_dep TEXT NOT NULL, PRIMARY KEY (ID_dep),FOREIGN KEY (ID_reg) REFERENCES Regions(ID_reg)")
# CreateTable("IndicNoms","ID_indic SMALLINT, libelle_indic TEXT NOT NULL, TYPE TEXT NOT NULL, PRIMARY KEY (ID_indic)")
# CreateTable("IndicReg","ID_reg SMALLINT,ID_indic SMALLINT, Value NUMERIC, annee_dbt DATE, annee_fin DATE, opt_para TEXT, PRIMARY KEY (ID_reg),FOREIGN KEY (ID_indic) REFERENCES IndicNoms(ID_indic)")
# CreateTable("IndicDEP","ID_dep SMALLINT,ID_indic SMALLINT, Value NUMERIC, annee_dbt DATE, annee_fin DATE, opt_para TEXT, PRIMARY KEY (ID_dep),FOREIGN KEY (ID_indic) REFERENCES IndicNoms(ID_indic)")


# #Insertion des valeurs
df_reg= pd.read_csv("./region2020.csv",sep =",", usecols = ['reg','libelle'])
df_dep= pd.read_csv("./departement2020.csv",sep =",", usecols = ['dep','reg','libelle'])
df_reg = CSV_typeCorrection(df_reg)
df_dep = CSV_typeCorrection(df_dep)
InsertInto("Regions","ID_reg,libelle_reg",df_reg)
InsertInto("Departements","ID_dep, ID_reg,libelle_dep",df_dep)


# commit et fin de la connection
cur.close()
conn.commit()
conn.close()
print("La connexion PostgreSQL est fermée")