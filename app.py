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
not_scanned_files = [] # (bank, account_id, fullpath)

for account_id,name,bank,path in accounts_folder:
    # récupérer les fichiers enregistrés
    scanned_files_sql = f"SELECT path FROM source_file WHERE account={account_id}"
    scanned_files = cur.execute(scanned_files_sql).fetchall()
    scanned_files = sum(scanned_files, ()) # passe de [(a,), (b,)] à (a,b)
    
    # récupérer les fichiers non scannés
    not_scanned = []
    for filename in os.listdir(path):
        n_file = 0
        p = os.path.join(path,filename)
        if os.path.isfile(p) and filename not in scanned_files:
            n_file += 1
            not_scanned_files.append((bank, account_id, p))
    
    if n_file:
        logging.info(f"{name} ({n_file} files not scanned)")

#------------------------ appel des fonctions de scan -------------------------

#   __main__  ------+
#      |            |
#      | fichiers   |
#      v            |
#  Extractor        | fichiers à traiter
#      |            |
#      | résultats  |
#      v            |
#  Réception  <-----+

# Pour chaque fichier :
# Extractor <- filepath & fonction à appeler (donc banque)
# Réception <- info_fichier + account_id + path & ops

from extraction.sogep import scan as sogep_scan
import queue
import threading

scan_functions = {
    "Société Générale": sogep_scan,
}

class Extractor(threading.Thread):
    def __init__(self, files_queue, ops_queue):
        self.files_q = files_queue
        self.ops_q = ops_queue
        super().__init__()
      
    def run(self):
        while True:
            try:
                file = self.files_q.get(timeout=3)
                bank, account_id, path = file
                result = scan_functions[bank](account_id, path)
            except queue.Empty:
                return
            except Exception as e:
                result = e

            self.ops_q.put([file,result])

class Reception(threading.Thread):
    def __init__(self, results_queue, files):
        self.results_q = results_queue 
        self.expected_files = files.copy()
        super().__init__()
      
    def run(self):
        while len(self.expected_files):
            file, result = self.results_q.get()
            self.expected_files.remove(file)
        
            if isinstance(result, Exception):
                logging.info("error on file: '%s'" % file[2])
                continue
            
            file_info,ops = result
        
            source_insert_sql = """INSERT INTO source_file 
            VALUES(:date_begin, :date_end, :date_scanned, :path, :account) 
            RETURNING rowid"""
            (source_id,) = cur.execute(source_insert_sql, file_info).fetchone()
            
            ops_insert_sql = f"""INSERT INTO operation 
            VALUES(:date, :payee, :motif, :label, :amount, :currency, NULL, {source_id})"""
            cur.executemany(ops_insert_sql, ops)

            filename = os.path.basename(file[2])
            logging.info("%s\t> %d ops, %s files left" % (filename, len(ops), len(self.expected_files)))


def extraction(files, n_threads=8):
    files_queue = queue.Queue()
    ops_queue = queue.Queue()
  
    for f in files:
        files_queue.put_nowait(f)
  
    extractors = [ Extractor(files_queue, ops_queue) for _ in range(n_threads) ]
    for t in extractors:
        t.start()
  
    reception = Reception(ops_queue, files)
    reception.start()
  
    for t in extractors:
        t.join()
    reception.join()

    con.commit()
    
extraction(not_scanned_files)

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

processing_func = {
    "set": lambda p,v: p,
    "before": lambda p,v: f"{p}{v if v != None else ''}",
    "after": lambda p,v: f"{v if v != None else ''}{p}",
    "replace": lambda p,v: v.replace(*p.split("|")),
    "re_replace": lambda p,v: re.sub(*p.split("|"),v),
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
            new_value = processing_func[method](value, to_value)
            if new_value != to_value:
                return new_value 
    
    return to_value

con.create_function("apply_rules", -1, apply_rules)

for root,_,files in os.walk(rules_path):
    folder = os.path.basename(root)
    for file in files:
        p = os.path.join(root,file)
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
                
                logging.info(f"{folder} applied ({file})")

# Banque/Mouvements : mouvements d'argent entre deux comptes perso
re_ref = re.compile(r"(?m)(?:VIR RECU|REF:) (?P<ref>\d*)")
ops = cur.execute("SELECT rowid,motif FROM operation WHERE category IS NULL AND payee = 'Damien Lesecq'").fetchall()

ref_table = []
for rowid,motif in ops:
    ref = re_ref.search(motif)
    if ref != None:
        ref_table.append((rowid, ref.group("ref"), motif))

ref_set = []
for rt in ref_table:
    for rs in ref_set:
        if rt[1] == rs[1]:
            cur.execute(f"UPDATE operation SET category='Banque/Mouvements' WHERE rowid={rt[0]}")
            cur.execute(f"UPDATE operation SET category='Banque/Mouvements' WHERE rowid={rs[0]}")
            break
    else:
        ref_set.append(rt)

con.commit()
logging.info("double operations marked")