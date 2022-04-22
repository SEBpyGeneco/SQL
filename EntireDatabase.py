'''
MANGANE Fatoumata  et RICHARD Sébastien

L'intégralité du traitement des 4 fichiers, incluant le chargement, le "parsing", 
la mise en forme dans des listes d'attributes, la création des tables et enfin l'insertion des valeurs est inclue dans ce script python.
Nous avons pris le parti de ne pas modifier les fichiers à la main. Tout est donc automatique.

1. Chargement des fichiers et stockage dans des dataframes.
    Les approches sont légèrement différentes selon les fichiers.
    Pour les deux fichiers .csv, nous avons simplement chargé tout le contenu avec la fonction 
    "read_csv" de panda. Pour les xlsx nous les avons d'abord transformé en csv avant de les lire.
2. Parsing
    Pour les fichiers .csv, une simple conversion des valeurs de type "numpy.int64" a été effectuée
    Pour les autres, il a fallu un vrai traitement, notamment retirer les indices de regions "P","M","F" et les valeurs NaN
3. liste d'attributs
    Les dataframes ont été parcourues lignes par lignes puis valeur par valeur et pour chacune de ces dernières,
    l'année (correspondant à la première case de la colonne), les indices de regions/departements (1ere case de la ligne),
    et les autres attributs nécessaires ont été automatiquement récupérés de la même manière.
    Ce n'est en revanche pas vrai pour les attributs "indices d'indicateurs", "complément" ou encore par exemple
    pour les intervalles d'années pour les indicateurs du fichier Evolution population.
    Ceux-ci ont été mis à la main dans des listes puis passés en argument dans des fonctions (voir ci-dessous).
    Les différents indicateurs ont par ailleurs également été distingués par des listes de numéros de colonnes.
4. Creations et insertion.
'''



############### 
# IMPORTATION #
############### 

import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
import math


############### 
#  FONCTIONS  #
############### 


## Chargement des fichiers .xls
def df_from_xls(path,sheetn):
    '''
    * convert a xls file into a panda dataframe
    * @param  {string} path -- path of the xlx file
    * @param  {string} sheetn -- name of the sheet in the xls file
    * @return {dataframe} df -- dataframe containing the data from the xls file
    '''
    read_file = pd.read_excel(path, sheet_name=sheetn)
    read_file.to_csv ("./new_csv.csv")
    df = pd.read_csv("./new_csv.csv", header=None)
    return df


## Parsing ; dans la 1ere fonction on retire des type "numpy.int64" qui bloque 
# l'insertion dans les tables ; dans la seconde on retire les valeurs abberantes
def df_numpyintCorrection(df):
    '''
    * convert a panda dataframe into a list of n-uplet and convert problemactic "numpy.int64" type into int
    * @param  {panda dataframe} df -- a dataframe
    * @return {list} new_rows -- list of n-uplet ready to be inserted into a table
    '''
    new_rows=[]
    for i in range (len(df.index)):
        new_row = list(df.iloc[i])
        for i in range(len(new_row)):
            if isinstance(new_row[i], np.generic):
               new_row[i] = int(new_row[i])
        new_rows.append(new_row)
    return new_rows

def list_problematicCharacters(list_nuplet):
    '''
    * Correction of problematic characters in a list of n-uplet
    * @param  {list} list_nuplet --
    * @return {list} list_nuplet  -- corrected list
    '''
    i = 0
    while i < len(list_nuplet):
        try:
            list_nuplet[i][0] = int(list_nuplet[i][0]) # needed in order to convert the "01" from the xls file into "1" so it's the same as in the .csv file
        except:
            pass
        if list_nuplet[i][0] == "P" or  list_nuplet[i][0]  == "M" or  list_nuplet[i][0]  == "F": # removes
            list_nuplet.pop(i)
            i -= 1
        # if the third element (the value) of an n-uplet is Nan
        if list_nuplet[i][2] == "nd" or list_nuplet[i][2] == "nd " or math.isnan(float(list_nuplet[i][2])): 
            # the n-uplet is also removed
            list_nuplet.pop(i) 
            i -= 1
        # convert the dates that are sometimes written as "2015.0" into 2015
        list_nuplet[i][3] = int(float(list_nuplet[i][3]))
        list_nuplet[i][4] = int(float(list_nuplet[i][4]))
        i += 1
    return list_nuplet

## Extraction de chaque valeur puis création de listes contenant tous les attributs nécessaires aux tables IndicRegions et IndicDepartements
# le principe des 4 fonctions suivantes ici est ainsi de
#   1. extraire à partir des dataframes (obtenues à partir des xlxs) les blocs de lignes contenant les valeurs
#   (dans le premier fichier xlsx avec social et eco, deux blocs par dataframe correspondant regions/departements sont conservés)
#   2. parcourir chaque valeur de ces lignes et les ajoutés dans une liste accompagné des attributs
#   (ID de la région, ID de l'indicateur, année de début, année de fin et eventuellement un complément)
def getCol(df,cols):
    '''
    * Extract columns from a df and set numerical index and header
    * @param  {dataframe} df --
    * @param  {list} cols -- a list of columns that are to be kept
    * @return {dataframe} df  -- simplified dataframe
    '''
    df.index = [x for x in range(len(df))]
    df = df[cols]
    df.columns = [x for x in range(len(df.columns))]
    return df

def Extract_Subset(df1,df2,cols):
    '''
    * Encapsulation of 'getCol' function for two dataframe
    * @param  {dataframe} df1 --
    * @param  {dataframe} df2 --
    * @param  {list} cols -- a list of columns that are to be kept
    * @return {tuple dataframe} df_reg,df_dep -- simplified dataframes
    '''
    df_reg = getCol(df1,cols)
    df_dep = getCol(df2,cols)
    return df_reg,df_dep

def newN_uplet(df,ID_indice,complement,anneeDBT,anneeEND):
    '''
    * Creates a list of n-uplet formatted with all the required attributes of the tables "IndicRegions" and "IndicDepartement" from a dataframe
    * @param  {dataframe} df --
    * @param  {string} ID_indice -- ID of indicator
    * @param  {string} complement -- optionnal attribute (can be empty)
    * @param  {string} anneeDBT -- starting date
    * @param  {string} anneeEND -- ending date (or same as starting date if not a period)
    * @return {} list_nuplet  -- list of n-uplet
    '''
    list_nuplet=[]
    for i in range (1,len(df.index)):
        row = list(df.iloc[i])
        for j in range(1,len(row)):
            # If annees not specified, then it is the year displayed at the beginning of the columns that is kept (for Social/Eco sheets, for EvolutionPop periods are manually specified)
            if anneeDBT != None: 
                list_nuplet.append([row[0],ID_indice,row[j],anneeDBT,anneeEND,complement])
            else:
                list_nuplet.append([row[0],ID_indice,row[j],df[j][0],df[j][0],complement])
    return  list_nuplet

def List_nuplet(df,regSTART,regEND,depSTART,depEND,list_cols,list_ind,list_complement,anneeDBT=[None] * 10,anneeEND=[None] * 10):
    '''
    * Encapsulation of the previous functions to generalize the process to an entiere sheet
    * @param  {dataframe} df -- original df
    * @param  {int} regSTART -- index number corresponding to the FIRST line where the "regions" information are found in the dataframe
    * @param  {int} regEND -- index number corresponding to the LAST line where the "regions" information are found in the dataframe
    * @param  {int} depSTART -- index number corresponding to the FIRST line where the "departements" information are found in the dataframe
    * @param  {int} depEND -- index number corresponding to the LAST line where the "departements" information are found in the dataframe
    * @param  {list} list_cols -- list of the columns number of every indicator
    * @param  {list} list_ind -- list of the for every indicator
    * @param  {list} list_complement -- list of optionnal attribute for every indicator
    * @param  {list} anneeDBT --  list of starting date for every indicator. Optionnal: if not specified, list of None
    * @param  {list} anneeEND -- list of ending date for every indicator. Optionnal: if not specified, list of None
    * @return {tuple dataframe} res_reg,res_dep  -- two list of list of n-uplets for 'regions' and 'departements'
    '''
    res_reg = []
    res_dep = []

    df_reg = df.iloc[regSTART:regEND,1:] # gives a reduced dataframe with only usefull information and indexed rows/columns
    df_dep = df.iloc[depSTART:depEND,1:]

    for i in range(len(list_cols)): # for every indicator
        df_indR,df_indD = Extract_Subset(df_reg,df_dep,list_cols[i]) # extraction of its corresponding columns for both 'regions' and 'departements'
        res_reg += newN_uplet(df_indR,list_ind[i],list_complement[i],anneeDBT[i],anneeEND[i]) # list concatlist of n-upletenation of all the n-uplets from this indicator to the general list
        res_dep += newN_uplet(df_indD,list_ind[i],list_complement[i],anneeDBT[i],anneeEND[i])
    return res_reg,res_dep

## Fonctions pour créer la table liant les indicateurs à leur ID ; dans la première on récupère les noms des indicateurs
# dans la seconde on créer la liste formatée avec tous les attributs de cette table
def getIndic(df,line):
    '''
    * Get a list of the indicators names
    * @param  {dataframe} df -- 
    * @param  {int} line -- the line in the dataframe where the names are specified
    * @return {list of string} res -- list of the indicators
    '''
    res = []
    indic = df.loc[line, :].values.tolist()
    for indice in indic:
        if isinstance(indice, str):
            res.append(indice)
    return res

def createIndN(indics,type,compteur=0):
    '''
    * Creates formatted n-uplet for the indicators table
    * @param  {list of string} indics -- the indicators names
    * @param  {string} type -- explanation of the type of the indicator ("Social","Eco"...)
    * @param  {int} compteur -- allows to use this function on several files and start to increment the indicators ID at the right number
    * @return {list} res -- list of list of n-uplet
    '''
    res = []
    for i in range(len(indics)):
        res.append([i+compteur,indics[i],type])
    return res

## Interaction avec database = creation de table et insertion
def CreateTable(nom,attributs):
    '''
    * Create a new table in the database and initialize its attributes
    * @param  {string} nom -- name of the table
    * @param  {string} attributs --  attributes of the table
    * @return {None}
    '''
    cur.execute(f"CREATE TABLE IF NOT EXISTS public.{nom}({attributs}); ")


def InsertInto(nom,attributs,list_newRows):
    '''
    * insert new n-uplets into an existing table 
    * @param  {string} nom -- name of the table
    * @param  {string} attributs --  attributes of the table
    * @param  {list} list_newRows --  list of list containing new rows to be inserted
    * @return {None}
    '''
    # creation of a %s string that has the right size
    subs = ""
    for i in range(len(list_newRows[0])):
        subs += "%s,"
    subs = subs[:-1]
    
    command = f"INSERT INTO {nom} ({attributs}) VALUES ({subs})"
    for value in list_newRows:
        try:
            cur.execute(command, value)
        except Exception as e :
            cur.close()
            conn.close()
            exit("error when running: " + command + " : " + str(e))



############### 
#    MAIN     #
############### 



## Connexion à la base de données
print("Connexion à la base de donnees...")
USERNAME="admin"
PASSWORD="admin"
DBN="Final?"
try:
    conn = psycopg2.connect(host="localhost", dbname=DBN,user=USERNAME,password=PASSWORD)
except Exception as e :
    exit("Connexion impossible à la base de donnees..." + str(e))
print("Connecté à la base de donnees...")
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)



## Création des tables
CreateTable("Regions","ID_reg SMALLINT,libelle_reg TEXT NOT NULL, PRIMARY KEY (ID_reg)")
CreateTable("Departements","ID_dep VARCHAR(3) NOT NULL, ID_reg SMALLINT, libelle_dep TEXT NOT NULL, PRIMARY KEY (ID_dep),FOREIGN KEY (ID_reg) REFERENCES Regions(ID_reg)")
CreateTable("IndicNoms","ID_indic SMALLINT, libelle_indic TEXT NOT NULL, type TEXT NOT NULL, PRIMARY KEY (ID_indic)")
CreateTable("IndicReg","ID_reg SMALLINT,ID_indic SMALLINT, Value NUMERIC, annee_dbt SMALLINT, annee_fin SMALLINT, opt_para TEXT, PRIMARY KEY (ID_reg,ID_indic,annee_dbt,annee_fin,opt_para),FOREIGN KEY (ID_indic) REFERENCES IndicNoms(ID_indic)")
CreateTable("IndicDEP","ID_dep VARCHAR(3) NOT NULL,ID_indic SMALLINT, Value NUMERIC, annee_dbt SMALLINT, annee_fin SMALLINT, opt_para TEXT, PRIMARY KEY (ID_dep,ID_indic,annee_dbt,annee_fin,opt_para),FOREIGN KEY (ID_indic) REFERENCES IndicNoms(ID_indic)")


## Chargement des fichiers
df_reg= pd.read_csv("/home/selen/Documents/BioInfo_Master/sql/fichiers/region2020.csv",sep =",", usecols = ['reg','libelle'])
df_dep= pd.read_csv("/home/selen/Documents/BioInfo_Master/sql/fichiers/departement2020.csv",sep =",", usecols = ['dep','reg','libelle'])
df_indSocial = df_from_xls('/home/selen/Documents/BioInfo_Master/sql/fichiers/DD-indic-reg-dep_2008_2019.xls','Social')
df_indEco = df_from_xls('/home/selen/Documents/BioInfo_Master/sql/fichiers/DD-indic-reg-dep_2008_2019.xls','Economie')
df_indEvoR= df_from_xls('/home/selen/Documents/BioInfo_Master/sql/fichiers/Evolution_population_2012-2020.xls','REG')
df_indEvoD= df_from_xls('/home/selen/Documents/BioInfo_Master/sql/fichiers/Evolution_population_2012-2020.xls','DEP')

## Insertion tables Regions et Departements
df_reg = df_numpyintCorrection(df_reg)
df_dep = df_numpyintCorrection(df_dep)
InsertInto("Regions","ID_reg,libelle_reg",df_reg)
InsertInto("Departements","ID_dep, ID_reg,libelle_dep",df_dep)


## Insertion tables IndicNoms

#get all the names
indSocial = getIndic(df_indSocial,4)
indEco = getIndic(df_indEco,4)
indEvo = getIndic(df_indEvoD,3)

indSocial.pop(0) # correction so that every population estimation have the same ID
indSocial[0] = "Espérance de vie à la naissance"
indSocial = createIndN(indSocial,"Social")

indEvo = indEvo[3:] # correction so that every population estimation have the same ID
indEvo[0] = "Estimation de la population (en millier)"
indEvo = createIndN(indEvo,"EvolutionPop",13)


indEco.pop(4) # Correction indice transports
indEco.pop(4)
indEco[4] = "Mode de transport pour se rendre au travail"
indEco[1] = "Taux demploi (%)"
indEco = createIndN(indEco,"Economie",6)

indEco[4][0] = 10
indEco[5][0] = 11
indEco[6][0] = 12
print(indEco)
print(indEvo)
InsertInto("IndicNoms","ID_indic, libelle_indic,type",indEco)
InsertInto("IndicNoms","ID_indic, libelle_indic,type",indSocial)
InsertInto("IndicNoms","ID_indic, libelle_indic,type",indEvo)


## Insertion dans tables IndicReg et IndicDep


# Social
list_cols = [ [1,3,4,5],
              [1,6,7,8],
              [1,9],
              [1,10,11],
              [1,12,13,14],
              [1,15],
              [1,16,17]]
list_ind = [0,0,1,2,3,4,5]
list_complements = ["M","F","","","","",""]

res_reg,res_dep = List_nuplet(df_indSocial,5,26,33,137,list_cols, list_ind, list_complements)

res_reg = list_problematicCharacters(res_reg)
res_dep = list_problematicCharacters(res_dep)

InsertInto("IndicReg","ID_reg,ID_indic, Value, annee_dbt, annee_fin, opt_para",res_reg)
InsertInto("IndicDep","ID_dep,ID_indic, Value, annee_dbt, annee_fin, opt_para",res_dep)

# Eco
list_cols = [ [1,3],
              [1,4,5],
              [1,6],
              [1,7,8],
              [1,9,10],
              [1,11,12],
              [1,13,14],
              [1,15],
              [1,16,17]]

list_ind = [6,7,8,9,10,10,10,11,12]

list_complements = ["","","","","Voiture","Transport en commun","autre moyen de transport","",""]

resEco_reg,resEco_dep = List_nuplet(df_indEco,5,26,35,139,list_cols, list_ind, list_complements)

resEco_reg = list_problematicCharacters(resEco_reg)
resEco_dep = list_problematicCharacters(resEco_dep)

InsertInto("IndicReg","ID_reg,ID_indic, Value, annee_dbt, annee_fin, opt_para",resEco_reg)
InsertInto("IndicDep","ID_dep,ID_indic, Value, annee_dbt, annee_fin, opt_para",resEco_dep)

### Evolution Pop

#Departements
list_cols = [ [1,3],
              [1,4],
              [1,5],
              [1,6],
              [1,7],
              [1,8],
              [1,9]]

list_ind = [13,13,13,13,14,15,16]

list_complements = [""]*7

anneesDBT = ["2012","2017","2018","2020","2012","0","0"]

anneesEND = ["2012","2017","2018","2020","2017","0","0"]

res_Evodep,_ = List_nuplet(df_indEvoD,3,105,106,107,list_cols, list_ind, list_complements,anneesDBT,anneesEND)
res_Evodep = list_problematicCharacters(res_Evodep)
InsertInto("IndicDep","ID_dep,ID_indic, Value, annee_dbt, annee_fin, opt_para",res_Evodep)

#Regions
list_cols = [ [1,3],
              [1,4],
              [1,5],
              [1,6],
              [1,7],
              [1,8]]

list_ind = [13,13,13,14,15,16]

list_complements = [""]*6

anneesDBT = ["2012","2017","2020","2012","0", "0"]

anneesEND = ["2012","2017","2020","2017","0","0"]

res_EvoR,_ = List_nuplet(df_indEvoR,3,22,23,24,list_cols, list_ind, list_complements,anneesDBT,anneesEND)
res_EvoR = list_problematicCharacters(res_EvoR)
InsertInto("IndicReg","ID_reg,ID_indic, Value, annee_dbt, annee_fin, opt_para",res_EvoR)


## Commit et close connection
cur.close()
conn.commit()
conn.close()
print("La connexion PostgreSQL est fermée")
