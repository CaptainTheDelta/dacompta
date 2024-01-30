import csv
import logging
import os
import re
import sqlite3
import toml

FORMAT = "[%(levelname)s:%(asctime)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=FORMAT)

#----------------------- connection à la base de donnée -----------------------

config = toml.load("config.toml")
to_populate = not os.path.exists(config['database']['path'])
con = sqlite3.connect(config['database']['path'],check_same_thread=False)
cur = con.cursor()

# création des tables
with open("scripts/init_db.sql", 'r') as script:
    cur.executescript(script.read())

# remplissage de la table des comptes
if to_populate:
    with open("scripts/data_injection.sql", 'r', encoding="utf-8") as script:
        cur.executescript(script.read())

# remplissage de la table des catégories
if True:
    with open("categories.csv", encoding="utf-8-sig") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        categories = list(csv_reader)
        cur.executemany("INSERT INTO category VALUES (?)", categories)

#----------------------------- scan des fichiers ------------------------------

accounts_folder_sql = "SELECT rowid,name,bank,folder FROM account"
accounts_folder = cur.execute(accounts_folder_sql).fetchall()
not_scanned_files = []

for account_id,name,_,path in accounts_folder:
    # récupérer les fichiers enregistrés
    scanned_files_sql = f"SELECT path FROM source_file WHERE account={account_id}"
    scanned_files = cur.execute(scanned_files_sql).fetchall()
    scanned_files = sum(scanned_files, ()) # passe de [(a,), (b,)] à (a,b)
    
    # récupérer les fichiers non scannés
    not_scanned = []
    for file in os.listdir(path):
        p = os.path.join(path,file)
        if os.path.isfile(p) and file not in scanned_files:
            not_scanned.append(file)

    not_scanned_files.append(not_scanned)
    
    if len(not_scanned):
        logging.info(f"{name} ({len(not_scanned)} files not scanned)")

#------------------------ appel des fonctions de scan -------------------------

from extraction.sogep import scan as sogep_scan
import threading

accounts_files = []

for (account_id,_,bank,folder),(files) in zip(accounts_folder,not_scanned_files):
    if bank == "Société Générale":
        accounts_files.append([account_id, folder, files])

sogep_scan(accounts_files, con, 10)

#------------------------------- règles custom --------------------------------

# pour chaque fichier de règles, application.
rules_path = config["rules"]["path"]
rules = []

criterion_func = {
    "equals": lambda p,v: p == v,
    "startswith": lambda p,v: isinstance(v, str) and v.startswith(p),
    "endswith": lambda p,v: isinstance(v, str) and v.endswith(p),
    "regex": lambda p,v: isinstance(v, str) and re.search(p,v) != None,
}

def check_criterion(value, pattern, method):
    if method not in criterion_func:
        return False
    return criterion_func[method](pattern,value)

def apply_rules(to_value, *from_values):
    for rule in rules:
        crits = [check_criterion(v, *c) for v,c in zip(from_values, rule[1])]
        
        if all(crits):
            value,method = rule[0]
            if method == "set":
                return value
            elif method == "begin":
                return f"{value}{to_value if to_value != None else ''}"
            elif method == "end":
                return f"{to_value if to_value != None else ''}{value}"
    
    return to_value


con.create_function("apply_rules", -1, apply_rules)

for file in os.listdir(rules_path):
    p = os.path.join(rules_path,file)
    if os.path.isfile(p):
        with open(p, encoding="utf-8-sig") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            header = next(csv_reader)
            n = len(header)
            rules = []
            
            for line in csv_reader:
                crits =  [tuple(line[i:i+2]) for i in range(2,n,2)]
                rules.append((tuple(line[:2]), crits))
            
            field_to = header[0]
            fields_from = ','.join(header[2::2])
            rules_sql = f"UPDATE operation SET {field_to}=apply_rules({field_to},{fields_from})"
            cur.execute(rules_sql)
            con.commit()
            
            logging.info(f"rules applied ({file})")