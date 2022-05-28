'''
MANGANE Fatoumata  et RICHARD Sébastien

Script python pour affichage d'un menu permettant d'intéragir avec la base de données.
'''


# IMPORTATION #
############### 

import psycopg2 
import psycopg2.extras


############### 
# CONNECTION  #
############### 

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

############### 
#  FONCTIONS  #
############### 

# affichage du menu principal et input du choix de l'utilisateur
def menu():
        print("")
        print("~~~~~~~~~~~~~~~~~~~~~")
        print(" Bienvenue sur le serveur ")
        print(" Choisissez une option ")
        print(" 1-Afficher la liste des régions ")
        print(" 2-Afficher les départements d'une région choisie ")
        print(" 3-Afficher les données relatives à un département et un thème ")
        print(" 4-Afficher la population annuelle d'un département ")
        print(" 5-Afficher les réponses aux autres requêtes... ")
        print(" 6-Quitter ")
        print("~~~~~~~~~~~~~~~~~~~~~")
        choice=int(input("Que souhaitez-vous faire ? "))
        return choice


# QUESTION 1 menu: "afficher la liste des régions"
def regions():
    command = 'SELECT libelle_reg FROM regions'
    try:
        cur.execute(command)
    except Exception as e :
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))

    regions = cur.fetchall() 
    if(not regions):
        print("Aucune région dans la liste? Vérifiez votre saisie... ")
    for region in regions:
        print(region[0])


# QUESTION 2 menu: "demander à l’utilisateur de choisir une région et afficher les départements de la région choisie"
def dep_from_reg():
        region = input("Choisissez une région: ")
        region = region.upper()
        command = f"SELECT libelle_dep FROM departements NATURAL JOIN regions where UPPER(libelle_reg) = '{region}'"
        try:
            cur.execute(command)
        except Exception as e :
            cur.close()
            conn.close()
            exit("error when running: " + command + " : " + str(e))

        departements = cur.fetchall() 
        if(not departements):
            print("Aucun département trouvé pour cette région? Vérifiez votre saisie... ")
        else:
            print(f" Votre région {region} comprends {len(departements)} départements: ")
            for departement in departements:
                print(departement[0])


# QUESTION 3 menu: "demander à l’utilisateur de choisir un département et un thème : social ou economique, et afficher les données demandées pour le département choisi."
def info_dep():
    departement = input("Choisissez un département: ")
    departement = departement.upper()
    choix_type = ['Social','EvolutionPop','Economie']
    type_input = int(input("Choissez un thème entre : Social(1), EvolutionPop(2) et Economie(3) "))
    type = choix_type[type_input-1]
    type = type.upper()
    print(f"SELECT libelle_indic, value, annee_dbt, annee_fin, opt_para  FROM indicdep NATURAL JOIN indicnoms NATURAL JOIN Departements WHERE UPPER(type) = '{type}' and UPPER(libelle_dep) = '{departement}'")
    command = f"SELECT libelle_indic, value, annee_dbt, annee_fin, opt_para  FROM indicdep NATURAL JOIN indicnoms NATURAL JOIN Departements WHERE UPPER(type) = '{type}' and UPPER(libelle_dep) = '{departement}'"
    try:
        cur.execute(command)
    except Exception as e :
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))

    infos = cur.fetchall()
    if(not infos):
        print("Aucune informations trouvé pour ce département? Vérifiez votre saisie... ")
    else:
        print(f"Le département {departement} présente:")
    for info in infos:
        if info[2] == info[3]:
            print(f" {info[0]}: {info[1]} pour l'année {info[2]} ({info[4]})")
        else:
            print(f" {info[0]}: {info[1]} de l'année {info[2]} à l'année {info[3]} {info[4]}")


# QUESTION 4 menu: "demander à l’utilisateur de choisir un département et une année et afficher la population du département cette année-là, si les données sont disponibles (sinon afficher « données non disponibles »)."
def pop_dep():
    departement = input("Choisissez un département: ")
    departement = departement.upper()
    annee = input("Choissez une année: ")
    param = 'Estimation de la population (en millier)'
    param = param.upper()
    command = f"SELECT value FROM departements NATURAL JOIN indicdep NATURAL JOIN indicnoms WHERE UPPER(libelle_indic) =  '{param}' AND UPPER(libelle_dep) = '{departement}' AND annee_dbt = '{annee}'"
    try:
        cur.execute(command)
    except Exception as e :
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))

    pop_values = cur.fetchall() 
    if(not pop_values):
        print("Données non disponibles, essayez une autre année")
    for pop_value in pop_values:
        print(f"le département {departement} avait une population de {pop_value[0]} (milliers) en {annee}")


# QUESTION 1: "afficher la liste des départements où le taux de pauvreté en 2018 était compris entre 15% et 20 %, classés du plus fort taux au plus faible."
def dep_pauvre():
    command = "SELECT libelle_dep, value FROM departements NATURAL JOIN indicdep NATURAL JOIN indicnoms WHERE libelle_indic = 'Taux de pauvreté (%) (1)' AND annee_dbt = 2018 AND value between 15 AND 20 ORDER BY value DESC;"
    try:
        cur.execute(command)
    except Exception as e :
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))

    deps = cur.fetchall() 
    if(not deps):
        print("Aucun departement dans la liste? Peut-être une erreur... ")
    else:
        print("Liste des départements avec un taux de pauvreté entre 15/20% affichée en ordre décroissant: ")
        for i in range (len(deps)):
            print(f" {i} Le departement {deps[i][0]}: {(deps[i][1])}")


# QUESTION 2: "Quels sont les départements dont la région avait un effort de recherche et développement strictement supérieur à 2 % en 2014 ? Afficher aussi le taux d’activité en 2017 pour ces départements."
def dep_RetD():
    command = "SELECT libelle_dep,value from Departements NATURAL JOIN indicreg NATURAL JOIN indicnoms WHERE annee_dbt = '2014' and libelle_indic = 'Effort de recherche et développement (%)' and value > 2"
    try:
        cur.execute(command)
    except Exception as e :
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))

    deps = cur.fetchall() 
    if(not deps):
        print("Aucun departement dans la liste? Peut-être une erreur... ")
    else:
        print("Liste des départements dont la région avait fait un effort de RD >2% en 2014  ")
        for i in range (len(deps)):
            print(f"Le departement {deps[i][0]} ({round(deps[i][1],2)})")

    command2 = "SELECT value from indicreg NATURAL JOIN departements NATURAL JOIN indicnoms WHERE libelle_indic = 'Taux d’activité (%)' and libelle_dep in (SELECT libelle_dep from Departements NATURAL JOIN indicreg NATURAL JOIN indicnoms WHERE annee_dbt = '2014' and libelle_indic = 'Effort de recherche et développement (%)' and value > 2);"
    try:
        cur.execute(command2)
    except Exception as e :
        cur.close()
        conn.close()
        exit("error when running: " + command2 + " : " + str(e))

    actis = cur.fetchall() 
    if(not actis):
        print("Rien à afficher? Surement une erreur... ")
    else:
        print("Ces mêmes départements ont des taux d'activités de:  ")
        for acti in actis:
            print(f"Le departement {acti[0]}")  


# QUESTION 3: "Afficher la différence entre l’espérance de vie des hommes et des femmes en 2019 pour tous les départements de la région ayant le plus grand taux de pauvreté en 2018."
def diff_H_F():
    commandH = "SELECT indicdep.value from departements NATURAL JOIN indicdep NATURAL JOIN indicnoms NATURAL JOIN regions JOIN indicreg on indicreg.id_reg = departements.id_reg WHERE indicdep.opt_para = 'M' AND libelle_indic = 'Espérance de vie à la naissance' AND indicdep.annee_dbt = '2019' AND indicreg.value = (select max(value) from Departements NATURAL JOIN indicreg NATURAL JOIN indicnoms WHERE libelle_indic = 'Taux de pauvreté (%) (1)' AND annee_dbt = '2018');"
    try:
        cur.execute(commandH)
    except Exception as e :
        cur.close()
        conn.close()
        exit("error when running: " + commandH + " : " + str(e))

    valH = cur.fetchall() 
    if(not valH):
        print("Aucun departement dans la liste? Peut-être une erreur... ")
        
    commandF = "SELECT indicdep.value, libelle_dep from departements NATURAL JOIN indicdep NATURAL JOIN indicnoms NATURAL JOIN regions JOIN indicreg on indicreg.id_reg = departements.id_reg WHERE indicdep.opt_para = 'F' AND libelle_indic = 'Espérance de vie à la naissance' AND indicdep.annee_dbt = '2019' AND indicreg.value = (select max(value) from Departements NATURAL JOIN indicreg NATURAL JOIN indicnoms WHERE libelle_indic = 'Taux de pauvreté (%) (1)' AND annee_dbt = '2018');"
    try:
        cur.execute(commandF)
    except Exception as e :
        cur.close()
        conn.close()
        exit("error when running: " + commandF + " : " + str(e))

    valF = cur.fetchall() 
    if(not valF):
        print("Aucun departement dans la liste? Peut-être une erreur... ")

    print(f" Il y a {len(valF)} département dont la région a le plus grand taux de pauvreté en 2018.")
    print(f" Il s'agit de {valF[0][1]}")
    print(f" Il y a une différence entre l’espérance de vie des hommes et des femmes en 2019 de {valF[0][0]-valH[0][0]} points de pourcentage")


# QUESTION 4: "Quelle est la population totale en 2017 de tous les départements où la part des jeunes non insérés (en 2017) est supérieure à 25% ? "
def pop_totale():
    command = "SELECT value, libelle_dep FROM departements NATURAL JOIN indicdep NATURAL JOIN indicnoms WHERE libelle_indic = 'Estimation de la population (en millier)' AND annee_dbt = '2017' AND libelle_dep IN (SELECT libelle_dep FROM departements NATURAL JOIN indicdep NATURAL JOIN indicnoms WHERE libelle_indic = 'Part des jeunes non insérés (%)' AND annee_dbt = '2017' AND value > 25);"
    try:
        cur.execute(command)
    except Exception as e :
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))

    deps_pop = cur.fetchall() 
    if(not deps_pop):
        print("Aucun departement dans la liste? Probablement une erreur... ")
    else:
        print("Liste de la population totale en 2017 de tous les départements où la part des jeunes non insérés (en 2017) est supérieure à 25%")
        for dep_pop in deps_pop:
            print(f" Le departement {dep_pop[1]} avait une population de {dep_pop[0]}")


# QUESTION 5: "En 2014, quelle était l’espérance de vie des femmes et des hommes dans les régions dont le taux d’emplois était supérieur à 63% et dont la part d’utilisation de la voiture pour se rendre au travail était moins de 75% ? "
def esp_vie():
    command = "SELECT value,libelle_reg FROM regions NATURAL JOIN indicreg NATURAL JOIN indicnoms  WHERE libelle_indic = 'Espérance de vie à la naissance' AND annee_dbt = '2015' AND libelle_reg IN (SELECT libelle_reg FROM regions NATURAL JOIN indicreg NATURAL JOIN indicnoms  WHERE libelle_indic = 'Taux demploi (%)' AND value > 63 INTERSECT  SELECT libelle_reg FROM regions NATURAL JOIN indicreg NATURAL JOIN indicnoms  WHERE libelle_indic = 'Mode de transport pour se rendre au travail'  AND opt_para = 'Voiture' AND value < 75);"
    try:
        cur.execute(command)
    except Exception as e :
        cur.close()
        conn.close()
        exit("error when running: " + command + " : " + str(e))

    vie_esp = cur.fetchall() 
    if(not vie_esp):
        print("Aucun résultat? Ca pourrait être une erreur... ")
    else:
        print("Dans les régions où le taux d’emplois était supérieur à 63% et dont la part d’utilisation de la voiture pour se rendre au travail était moins de 75% en 2014: ")
        for i in range(len(vie_esp)):
            if i<len(vie_esp)/2:
                print(f" L'espérance de vie des hommes était de {vie_esp[i][0]} (2015) dans la région {vie_esp[i][1]}")
            else:
                print(f" L'espérance de vie des femmes était de {vie_esp[i][0]} (2015) dans la région {vie_esp[i][1]}")

def retour():
    print("Retour à l'accueil")

def quit():
    print("Quitter...")
    cur.close()
    conn.close()

def autres_req():
    print("")
    print("~~~~~~~~~~~~~~~~~~~~~")
    print(" Choisissez une requête à afficher parmi celles ci-dessous: ")
    print(" 1-afficher la liste des départements où le taux de pauvreté en 2018 était compris entre 15% et 20 %, classés du plus fort taux au plus faible ")
    print(" 2-quels sont les départements dont la région avait un effort de recherche et développement strictement supérieur à 2 % en 2014 ? Afficher aussi le taux d’activité en 2017 pour ces départements ")
    print(" 3-afficher la différence entre l’espérance de vie des hommes et des femmes en 2019 pour tous les départements de la région ayant le plus grand taux de pauvreté en 2018. ")
    print(" 4-Quelle est la population totale en 2017 de tous les départements où la part des jeunes non insérés (en 2017) est supérieure à 25% ?  ")
    print(" 5-En 2014, quelle était l’espérance de vie des femmes et des hommes dans les régions dont le taux d’emplois était supérieur à 63% et dont la part d’utilisation de la voiture pour se rendre au travail était moins de 75% ")
    print(" 6-Revenir au menu principal")
    print("~~~~~~~~~~~~~~~~~~~~~")
    possibilite = [dep_pauvre,dep_RetD,diff_H_F,pop_totale,esp_vie,retour]
    choix_req =int(input("Que souhaitez-vous faire ? "))
    action = possibilite[choix_req-1]()


############### 
#    MAIN     #
############### 

options = [regions,dep_from_reg,info_dep,pop_dep,autres_req,quit]
choice = 0
while choice != 6:
    choice = menu()
    if choice not in [1,2,3,4,5,6]:
        print("Votre choix n'est pas valide, réessayez")
    else:
        options[choice-1]()
