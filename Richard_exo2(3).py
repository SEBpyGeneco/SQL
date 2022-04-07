import pandas as pd
import psycopg2 # Python SQL driver for PostgreSQL
import psycopg2.extras
import numpy as np
import unidecode

###### Fonctions #####

def displayInfo(dataframe):
    print(dataframe.columns)
    print(len(dataframe.index))
    print(dataframe.head(10))


def createTable(table_name,attributs):
    # Try to connect to an existing database
    print("Connexion à la base de donnees...")
    USERNAME="serichard"
    PASSWORD="DataSQL55"
    try:
        conn = psycopg2.connect(host="pgsql", dbname=USERNAME,user=USERNAME,password=PASSWORD)
    except Exception as e :
        exit("Connexion impossible à la base de donnees..." + str(e))
    print("Connecté à la base de donnees...")

    # préparation de l’exécution des requêtes (à ne faire qu’une fois)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(f"CREATE TABLE public.{table_name}({attributs}); ")
    cur.close()
    conn.commit()
    conn.close()
    print("La connexion PostgreSQL est fermée")


def insertInto(table_name,attributs,nbr_attr,new_values):
    # Try to connect to an existing database
    print("Connexion à la base de donnees...")
    USERNAME="serichard"
    PASSWORD="DataSQL55"
    try:
        conn = psycopg2.connect(host="pgsql", dbname=USERNAME,user=USERNAME,password=PASSWORD)
    except Exception as e :
        exit("Connexion impossible à la base de donnees..." + str(e))
    print("Connecté à la base de donnees...")

    # préparation de l’exécution des requêtes (à ne faire qu’une fois)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    val = ""
    for i in range(nbr_attr):
        val += "%s,"
    val = val[:-1]
    
    command = f"INSERT INTO {table_name} ({attributs}) VALUES ({val})"
    for value in new_values:
        try:
            cur.execute(command, value)
        except Exception as e :
            cur.close()
            conn.close()
            exit("error when running: " + command + " : " + str(e))
    cur.close()
    conn.commit()
    conn.close()
    print("La connexion PostgreSQL est fermée")


def IterateDF(dataframe):
    rows=[]
    for i in range (len(dataframe.index)):
        new_row = list(dataframe.iloc[i])
        for i in range(len(new_row)):
            if isinstance(new_row[i], float):
                new_row[i] = str(new_row[i])
            elif isinstance(new_row[i], np.generic):
               new_row[i] = int(new_row[i])
        rows.append(new_row)
    return rows

#### Main ####

dfParc= pd.read_csv("/net/cremi/serichard/espaces/travail/sql/ec_parc_s.csv",sep =";")
dfArret= pd.read_csv("/net/cremi/serichard/espaces/travail/sql/sv_arret_p.csv",sep =";")

displayInfo(dfParc)
displayInfo(dfArret)

# print(IterateDF(dfParc))

print("Connexion à la base de donnees...")
USERNAME="serichard"
PASSWORD="DataSQL55"
try:
    conn = psycopg2.connect(host="pgsql", dbname=USERNAME,user=USERNAME,password=PASSWORD)
except Exception as e :
    exit("Connexion impossible à la base de donnees..." + str(e))
print("Connecté à la base de donnees...")

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# createTable("parcs","ident INT PRIMARY KEY NOT NULL, insee INT,nom TEXT,description TEXT, milieu TEXT,paysage TEXT, service TEXT")
# new_rows = IterateDF(dfParc)
# insertInto("parcs","ident,insee,nom,description,milieu,paysage,service",7,new_rows)

# createTable("arretsbus","ident TEXT PRIMARY KEY NOT NULL, LIBELLE TEXT,VEHICULE TEXT,VOIRIE TEXT, INSEE TEXT")
# new_rows2 = IterateDF(dfArret)
# insertInto("arretsbus","ident,LIBELLE,VEHICULE,VOIRIE,INSEE",5,new_rows2)



#### Exercice a) ####

command = "SELECT nom,milieu,service from parcs JOIN arretsbus ON UPPER(parcs.nom) LIKE CONCAT('%', UPPER(arretsbus.libelle),'%') WHERE parcs.service LIKE CONCAT('%','HANDICAPES_PARTIEL','%') OR parcs.service LIKE CONCAT('%','HANDICAPES_TOTAL','%');"
cur.execute(command)
parcs_accesH = cur.fetchall()
if(not parcs_accesH):
    print("No result found")
for r in parcs_accesH:
    print(f"le parc {r[0]} a un acces handicapé")


env = input("Donnez un environnement (par exemple foret, bois, vigne, coteaux, cours d’eau, ...): ")
env = env.replace("'","")
env = env.split()
if (len(env)>0):
    env = env[0]


command = f"SELECT nom,milieu,paysage from parcs WHERE milieu LIKE CONCAT('%', UPPER('{env[:-1]}'),'%') OR paysage LIKE CONCAT('%', UPPER('{env[:-1]}'),'%');"
cur.execute(command)
parcs_env = cur.fetchall()
if(not parcs_env):
    print("No result found")
for r in parcs_env:
    print("le parc",r[0]," correspond à votre environnement")


# command = "SELECT nom,milieu,service from parcs JOIN arretsbus ON UPPER(parcs.nom) LIKE CONCAT('%', UPPER(arretsbus.libelle),'%') WHERE parcs.service LIKE CONCAT('%','HANDICAPES_PARTIEL','%') OR parcs.service LIKE CONCAT('%','HANDICAPES_TOTAL','%');"
# cur.execute(command)

# dataF_parcs = pd.read_sql_query(command,conn)

# writer = pd.ExcelWriter("dataF_parcs.xlsx", engine="xlsxwriter")
# dataF_parcs.to_excel(writer, sheet_name="Sheet1")
# writer.save()

# cur.close()
# conn.commit()
# conn.close()
# print("La connexion PostgreSQL est fermée")