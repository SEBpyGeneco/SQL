##### IMPORTATION #####
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
import math

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

def df_from_xls(path,sheetn,separateur=","):
    read_file = pd.read_excel (path, sheet_name=sheetn)
    read_file.to_csv ("./new_csv.csv")
    df = pd.read_csv("./new_csv.csv",sep =separateur, header=None)
    return df

def newN_uplet(df,nomIndice,complement,anneeDBT,anneeEND):
    n_uplet=[]
    for i in range (1,len(df.index)):
        row = list(df.iloc[i])
        # print(i,row)
        for j in range(1,len(row)):
            # print(j,row[j])
            annee=str(df[j][0])
            if len(annee) > 8:
                n_uplet.append([row[0],nomIndice,row[j],anneeDBT,anneeEND,complement])
            else:
                n_uplet.append([row[0],nomIndice,row[j],annee,annee,complement])
    return n_uplet

def getCol(df,cols):
    df.index = [x for x in range(len(df))]
    df = df[cols]
    df.columns = [x for x in range(len(df.columns))]
    return df

def Extract_Subset(df1,df2,cols):
    df_reg = getCol(df1,cols)
    df_dep = getCol(df2,cols)
    return df_reg,df_dep

def List_nuplet(df,regSTART,regEND,depSTART,depEND,list_cols,list_ind,list_complement,anneeDBT=[None] * 10,anneeEND=[None] * 10):
    res_reg = []
    res_dep = []

    df_reg = df.iloc[regSTART:regEND,1:]
    df_dep = df.iloc[depSTART:depEND,1:]

    for i in range(len(list_cols)):
        df_indR,df_indD = Extract_Subset(df_reg,df_dep,list_cols[i])
        res_reg += newN_uplet(df_indR,list_ind[i],list_complement[i],anneeDBT[i],anneeEND[i])
        res_dep += newN_uplet(df_indD,list_ind[i],list_complement[i],anneeDBT[i],anneeEND[i])
    return res_reg,res_dep

def getIndic(df,line):
    res = []
    indic = df.loc[line, :].values.tolist()
    for indice in indic:
        if isinstance(indice, str):
            res.append(indice)
    return res

def createIndN(indics,type,compteur=0):
    res = []
    for i in range(len(indics)):
        res.append([i+compteur,indics[i],type])
    return res

def correctionParsing(liste):
    i = 0
    while i < len(liste):
        try:
            int(liste[i][0])
        except:
            liste.pop(i)
            i -= 1
        if liste[i][2] == "nd" or liste[i][2] == "nd " or math.isnan(float(liste[i][2])):
            liste.pop(i)
            i -= 1
        liste[i][3] = int(float(liste[i][3]))
        liste[i][4] = int(float(liste[i][4]))
        i += 1
    return liste

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

#Création des tables
CreateTable("Regions","ID_reg SMALLINT,libelle_reg TEXT NOT NULL, PRIMARY KEY (ID_reg)")
CreateTable("Departements","ID_dep VARCHAR(3) NOT NULL, ID_reg SMALLINT, libelle_dep TEXT NOT NULL, PRIMARY KEY (ID_dep),FOREIGN KEY (ID_reg) REFERENCES Regions(ID_reg)")
CreateTable("IndicNoms","ID_indic SMALLINT, libelle_indic TEXT NOT NULL, type TEXT NOT NULL, PRIMARY KEY (ID_indic)")
CreateTable("IndicReg","ID_reg SMALLINT,ID_indic SMALLINT, Value NUMERIC, annee_dbt INT, annee_fin INT, opt_para TEXT, PRIMARY KEY (ID_reg,ID_indic,annee_dbt,annee_fin,opt_para),FOREIGN KEY (ID_indic) REFERENCES IndicNoms(ID_indic)")
CreateTable("IndicDEP","ID_dep SMALLINT,ID_indic SMALLINT, Value NUMERIC, annee_dbt INT, annee_fin INT, opt_para TEXT, PRIMARY KEY (ID_dep,ID_indic,annee_dbt,annee_fin,opt_para),FOREIGN KEY (ID_indic) REFERENCES IndicNoms(ID_indic)")


# Insertion des valeurs
df_reg= pd.read_csv("/home/selen/Documents/BioInfo_Master/sql/fichiers/region2020.csv",sep =",", usecols = ['reg','libelle'])
df_dep= pd.read_csv("/home/selen/Documents/BioInfo_Master/sql/fichiers/departement2020.csv",sep =",", usecols = ['dep','reg','libelle'])
df_reg = CSV_typeCorrection(df_reg)
df_dep = CSV_typeCorrection(df_dep)
InsertInto("Regions","ID_reg,libelle_reg",df_reg)
InsertInto("Departements","ID_dep, ID_reg,libelle_dep",df_dep)

############ social+eco

df_indSocial = df_from_xls('/home/selen/Documents/BioInfo_Master/sql/fichiers/DD-indic-reg-dep_2008_2019.xls','Social')
df_indEco = df_from_xls('/home/selen/Documents/BioInfo_Master/sql/fichiers/DD-indic-reg-dep_2008_2019.xls','Economie')
df_indEvoR= df_from_xls('/home/selen/Documents/BioInfo_Master/sql/fichiers/Evolution_population_2012-2020.xls','REG')
df_indEvoD= df_from_xls('/home/selen/Documents/BioInfo_Master/sql/fichiers/Evolution_population_2012-2020.xls','DEP')

####table indicateurs
indSocial = getIndic(df_indSocial,4)
indEco = getIndic(df_indEco,4)
indEvoD = getIndic(df_indEvoD,3)
indEvoD.pop(0) # Correction indice population au 1er janvier
indEvoD.pop(0)
indEvoD.pop(0)

indSocial = createIndN(indSocial,"Social")
indSocial.pop(0) # Correction indice homme/femme
indSocial[0][1] = "Espérance de vie à la naissance"

indEco = createIndN(indEco,"Economie",7)
indEco[7][0] = 12 # Correction indice transports
indEco[8][0] = 13 
indEco.pop(4)
indEco.pop(4)
indEco[4][1] = "Mode de transport pour se rendre au travail"
indEco[4][0] = 11

indics = indSocial + indEco
indEvoD = createIndN(indEvoD,"EvolutionPop",14)
indEvoD[0][1] = "EStimation de la population (en millier)"

InsertInto("IndicNoms","ID_indic, libelle_indic,type",indics)
InsertInto("IndicNoms","ID_indic, libelle_indic,type",indEvoD)




# table IndicReg et Dep pour Social


list_cols = [ [1,3,4,5],
              [1,6,7,8],
              [1,9],
              [1,10,11],
              [1,12,13,14],
              [1,15],
              [1,16,17]]
list_ind = [ 1,
              1,
              2,
              3,
              4,
              5,
              6]
list_complements = ["M",
                    "F",
                    "",
                    "",
                    "",
                    "",
                    ""]

res_reg,res_dep = List_nuplet(df_indSocial,5,26,33,137,list_cols, list_ind, list_complements)

res_reg = correctionParsing(res_reg)
res_dep = correctionParsing(res_dep)

InsertInto("IndicReg","ID_reg,ID_indic, Value, annee_dbt, annee_fin, opt_para",res_reg)
InsertInto("IndicDep","ID_dep,ID_indic, Value, annee_dbt, annee_fin, opt_para",res_dep)

# table IndicReg et Dep pour Eco

list_cols = [ [1,3],
              [1,4,5],
              [1,6],
              [1,7,8],
              [1,9,10],
              [1,11,12],
              [1,13,14],
              [1,15],
              [1,16,17]]

list_ind = [ 7,
              8,
              9,
              10,
              11,
              11,
              11,
              12,
              13]

list_complements = ["",
                    "",
                    "",
                    "",
                    "Voiture",
                    "Transport en commun",
                    "autre moyen de transport",
                    "",
                    ""]

resEco_reg,resEco_dep = List_nuplet(df_indEco,5,26,35,139,list_cols, list_ind, list_complements)

resEco_reg = correctionParsing(resEco_reg)
resEco_dep = correctionParsing(resEco_dep)

InsertInto("IndicReg","ID_reg,ID_indic, Value, annee_dbt, annee_fin, opt_para",resEco_reg)
InsertInto("IndicDep","ID_dep,ID_indic, Value, annee_dbt, annee_fin, opt_para",resEco_dep)

### Evolution Pop

list_cols = [ [1,3],
              [1,4],
              [1,5],
              [1,6],
              [1,7],
              [1,8],
              [1,9]]

list_ind = [ 14,
            14,
            14,
            14,
              15,
              16,
              17]

list_complements = ["",
                    "",
                    "",
                    "",
                    "",
                    "",
                    ""]

anneesDBT = ["2012",
                    "2017",
                    "2018",
                    "2020",
                    "2012",
                    "0",
                    "0"]

anneesEND = ["2012",
                    "2017",
                    "2018",
                    "2020",
                    "2017",
                    "0",
                    "0"]


res_Evodep,_ = List_nuplet(df_indEvoD,3,105,106,107,list_cols, list_ind, list_complements,anneesDBT,anneesEND)
res_Evodep = correctionParsing(res_Evodep)
InsertInto("IndicDep","ID_dep,ID_indic, Value, annee_dbt, annee_fin, opt_para",res_Evodep)


list_cols = [ [1,3],
              [1,4],
              [1,5],
              [1,6],
              [1,7],
              [1,8]]

list_ind = [ 14,
            14,
            14,
              15,
              16,
              17]

list_complements = ["",
                    "",
                    "",
                    "",
                    "",
                    ""]

anneesDBT = ["2012",
                    "2017",
                    "2020",
                    "2012",
                    "0",
                    "0"]

anneesEND = ["2012",
                    "2017",
                    "2020",
                    "2017",
                    "0",
                    "0"]


res_EvoR,_ = List_nuplet(df_indEvoR,3,22,23,24,list_cols, list_ind, list_complements,anneesDBT,anneesEND)
res_EvoR = correctionParsing(res_EvoR)
InsertInto("IndicReg","ID_reg,ID_indic, Value, annee_dbt, annee_fin, opt_para",res_EvoR)



# commit et fin de la connection
cur.close()
conn.commit()
conn.close()
print("La connexion PostgreSQL est fermée")