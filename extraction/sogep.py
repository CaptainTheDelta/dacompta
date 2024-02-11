import logging
import os
import re
import subprocess
import tempfile
import threading

from datetime import datetime

def gettext(path):
    # génère un dossier temporaire
    with tempfile.TemporaryDirectory() as tmp:
        tmp_filepath = os.path.join(tmp, 'fitz-extracted')
        
        subprocess.run([
            *"python -m fitz gettext -m layout -o".split(),
            tmp_filepath,
            path
        ])

        with open(tmp_filepath, 'r', encoding="utf-8") as tmp_file:
            data = tmp_file.read()

    return data.rstrip()

re_dates = re.compile(r"^\s*du (?P<date_begin>\d{2}/\d{2}/\d{4}) au (?P<date_end>\d{2}/\d{2}/\d{4})$", re.MULTILINE)

re_ops_begin = re.compile(r"^\s*\d{2}/\d{2}/\d{4}(\s*\d{2}/\d{2}/\d{4})?", re.MULTILINE)
re_ops_end = re.compile(r"(suite >>>$)|(\s*TOTAUX DES MOUVEMENTS)|(^ *1 Depuis l'étranger :)", re.MULTILINE)
re_ops_header = re.compile(r"^\s*Date\s*Valeur\s*Nature de l'opération\s*Débit\s*(?P<crédit>Crédit)")
re_ops_amount = re.compile(r"(?P<number>(\d{1,3}(?:\.\d{3})*,\d{2}))\s*\*?$")

re_annoying_whitespace = re.compile("\s+")
re_annoying_star = re.compile("\s{2,}\*$", re.MULTILINE)

re_motif_card = re.compile(r"^CARTE (?P<card>X\d{4}) (?:REMBT )?(?P<date>\d{2}/\d{2})(?:\s\d{2}H\d{2})?\s(?P<payee>(?P<label>.*?))(?:\s*\d{1,},\d{2} EUR [A-Z\-]{1,})?(?:\s*COMMERCE ELECTRONIQUE)?$", re.S)
re_motif_vir = re.compile(r"POUR: (?P<payee>.*?)(\s\d{2} \d{2}.*?)?(\sDATE: (?P<date>.*?))?(\sREF: .*?)(\sMOTIF: (?P<label>.*?))?(\sLIB: .*)?(\sCHEZ: .*)?$", re.S)
re_motif_vir2 = re.compile(r"DE: ?(?P<payee>.*?)(\sID: .*?)?(\sDATE: (?P<date>.*?))?(\sMOTIF: (?P<label>.*?))?(\sVotre RUM: .*?)?(\sREF: .*?)?$", re.S)

re_motif = [
    re_motif_card,
    re_motif_vir,
    re_motif_vir2,
]

re_motif_sogep_fee = re.compile(r"^(FRAIS|COMMISSION D'INTERVENTION|LETTRE INFO)")
re_motif_sogep_interest = re.compile(r"^INTERETS")
re_motif_sogep_cotisation = re.compile(r"^COTISATION JAZZ")
re_motif_sogep_gab = re.compile(r"^VRST GAB")
re_motif_sogep_dab = re.compile(r"^CARTE X\d{4} RETRAIT DAB")

sogep_specials = {
    re_motif_sogep_fee: "Frais bancaires",
    re_motif_sogep_interest: "Intérêts",
    re_motif_sogep_cotisation: "COTISATION JAZZ -25 ANS -50%",
    re_motif_sogep_gab: "Dépôt GAB"
}

def scan(account_id, path):
    text = gettext(path)
    endpage = ""
    pages = text.split(endpage)

    # obtention des différentes dates du fichier
    file_info = {
        'account': account_id,
        'date_scanned': datetime.now(),
        'date_begin': None,
        'date_end': None,
        'path': os.path.basename(path),
    }
    for line in pages[0].splitlines():
        d = re_dates.search(line)
        if d != None:
            file_info['date_begin'] = d.groupdict().get('date_begin')
            file_info['date_end'] = d.groupdict().get('date_end')
            break

    ops = []

    for page in pages:        
        # réduction à la zone des opérations
        ops_begin = re_ops_begin.search(page)
        if ops_begin == None:
            continue

        start = ops_begin.start()
        end = re_ops_end.search(page).start()

        # séparation des opérations
        extracted_ops = []
        for line in page[start:end].splitlines():
            if re_ops_begin.match(line):
                extracted_ops.append([line])
            elif line.strip().startswith("***") or line.strip() == '':
                continue
            else:
                extracted_ops[-1].append(line)

        if len(extracted_ops) == 0:
            continue

        # détermination des indices de colonne
        for line in reversed(page[:start].splitlines()):
            m = re_ops_header.match(line)
            if m != None:
                sign_indice = re_ops_header.search(line).end("crédit")
                break

        # interpretation première des opérations
        for extracted_op in extracted_ops:
            first_line = extracted_op[0]
            amount_match = re_ops_amount.search(first_line)
            amount_start = amount_match.start()

            amount = float(amount_match.group('number').replace('.', '').replace(',', '.'))

            first_line = re_annoying_whitespace.sub(' ', first_line[:amount_start].strip())
            
            if len(extracted_op[0]) < sign_indice:
                amount *= -1
            
            date = datetime.strptime(first_line[:10], "%d/%m/%Y").date()
            motif = first_line[21:amount_start].strip()

            for line in extracted_op[1:]:
                line = re_annoying_star.sub('', line).strip()
                line = re_annoying_whitespace.sub(' ', line)
                if line != '':
                    motif += '\n' + line

            motif = motif.strip("\n")
            
            op = {
                'currency': "EUR",
                'date': date,
                'payee': None,
                'motif': motif,
                'label': None,
                'amount': amount,
            }

            # motif mining
            for re_m in re_motif:
                m = re_m.search(motif)
                if m != None:
                    op['payee'] = m.groupdict().get('payee')
                    op['label'] = m.groupdict().get('label')
            
            if re_motif_sogep_dab.match(motif):
                op['payee'] = "Damien Lesecq"
                op['label'] = "Retrait DAB"

            for re_m in sogep_specials:
                if re_m.match(motif):
                    op['payee'] = "Société Générale"
                    op['label'] = sogep_specials[re_m]

            ops.append(op)
    return file_info, ops