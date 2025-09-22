# src/utils.py
"""
Parsing eficiente do arquivo SNAP (amazon-meta.txt)

Funções exportadas:
- extract_all_categories(path) -> dict old_id -> {"name": name, "parent_old_id": parent_old_id}
    Realiza uma passagem rápida pelo arquivo apenas para coletar todas as categorias (nome + old_id + parent_old_id).
    Isso permite inserir categorias no banco uma vez, mapear old_id -> new_id e só depois atualizar parent_id.

- parse_snap(path)
    Iterador (generator) que devolve produto por produto na forma de dicionário:
    {
      "id": int,
      "asin": str,
      "title": str,
      "group": str,
      "salesrank": int or None,
      "similar": [asin,...],
      "categories": [ {"old_id": int, "name": str, "parent_old_id": int or None}, ... ],
      "reviews": [ {"date": date or None, "customer": str, "rating": int, "votes": int, "helpful": int}, ... ]
    }

Implementações feitas com foco em:
- robustez de parsing (reviews sem indentação também são reconhecidas),
- uso de operações leves (regex simples e operações de string),
- evitando manter em memória coisas desnecessárias em massa (parse_snap é streaming).
"""

import re
from datetime import datetime

# Regex para linhas de review (aceita várias quantidades de espaços entre campos)
REVIEW_RE = re.compile(
    r'^\s*(\d{4}-\d{1,2}-\d{1,2})\s+cutomer:?\s*([^\s]+)\s+rating:\s*(\d+)\s+votes:\s*(\d+)\s+helpful:\s*(\d+)',
    flags=re.IGNORECASE
)

# Regex para categorias estilo: |Books[283155]|Subjects[1000]|...
CAT_PART_RE = re.compile(r'\|([^|\[]+)\[(\d+)\]')

def extract_all_categories(path):
    """
    Primeira passagem: percorre todo o arquivo e extrai todas as categorias
    com seus old_ids e o parent_old_id imediato.

    Retorna:
        dict old_id (int) -> {"name": name (str), "parent_old_id": parent_old_id (int or None)}
    Observações:
    - Se a mesma old_id aparecer com nomes diferentes, a primeira aparição é preservada.
    """
    categories = {}  # old_id -> {"name":..., "parent_old_id": ...}

    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        for raw in f:
            line = raw.rstrip('\n')
            # procuramos linhas que contenham '|' categorias (linhas geralmente começam por '|')
            if '|' in line and '[' in line and ']' in line:
                # achar todas as partes com regex
                parts = CAT_PART_RE.findall(line)
                # parts: list of tuples (name, id_str)
                prev_old_id = None
                for name, id_str in parts:
                    try:
                        old_id = int(id_str)
                    except:
                        continue
                    name = name.strip()
                    # se não existe, registramos
                    if old_id not in categories:
                        categories[old_id] = {"name": name, "parent_old_id": prev_old_id}
                    # se já existe, não sobrescrever (preserva a primeira aparição)
                    prev_old_id = old_id
    return categories


def _parse_category_line_to_list(line):
    """
    Converte a linha de categorias em uma lista de dicts com old_id, name e parent_old_id.
    Exemplo de entrada:
       "|Books[283155]|Subjects[1000]|Literature & Fiction[17]|Drama[2159]|United States[2160]"
    Retorna:
       [
         {"old_id":283155, "name":"Books", "parent_old_id": None},
         {"old_id":1000, "name":"Subjects", "parent_old_id":283155},
         ...
       ]
    """
    parts = CAT_PART_RE.findall(line)
    out = []
    prev = None
    for name, id_str in parts:
        try:
            old_id = int(id_str)
        except:
            continue
        name = name.strip()
        out.append({"old_id": old_id, "name": name, "parent_old_id": prev})
        prev = old_id
    return out


def parse_snap(path):
    """
    Iterador que gera um dicionário por produto, de forma streaming (não carrega tudo em memória).
    Cada produto tem:
        'id', 'asin', 'title', 'group', 'salesrank', 'similar' (list of asins),
        'categories' (list of dicts with old_id, name, parent_old_id),
        'reviews' (list of dicts)
    Observações:
    - As linhas de review são detectadas via regex REVIEW_RE, sem dependência de indentação.
    - A função é tolerante a linhas estranhas/formatos levemente diferentes.
    """
    product = None

    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        for raw in f:
            line = raw.rstrip('\n')
            if line.startswith('Id:'):
                # finaliza produto anterior
                if product:
                    yield product
                product = {
                    'id': int(line.split(':', 1)[1].strip()),
                    'asin': None,
                    'title': None,
                    'group': None,
                    'salesrank': None,
                    'similar': [],
                    'categories': [],  # list of dicts (old_id, name, parent_old_id)
                    'reviews': []
                }
            elif product is None:
                # antes do primeiro Id:, ignorar
                continue
            elif line.startswith('ASIN:'):
                product['asin'] = line.split(':', 1)[1].strip()
            elif line.startswith('title:'):
                product['title'] = line.split(':', 1)[1].strip()
            elif line.startswith('group:'):
                product['group'] = line.split(':', 1)[1].strip()
            elif line.startswith('salesrank:'):
                v = line.split(':', 1)[1].strip()
                try:
                    product['salesrank'] = int(v)
                except:
                    product['salesrank'] = None
            elif line.startswith('similar:'):
                parts = line.split()
                # linha: similar: 5  B001... B002... ...
                if len(parts) > 2:
                    product['similar'] = parts[2:]
            elif line.startswith('categories:'):
                # a próxima(s) linha(s) com '|' contêm as categorias reais; ignorar aqui, tratar na linha seguinte
                pass
            elif '|' in line and '[' in line and ']' in line:
                # linha de categorias
                cats = _parse_category_line_to_list(line)
                # evitar duplicatas exatas
                existing_old_ids = {c['old_id'] for c in product['categories']}
                for c in cats:
                    if c['old_id'] not in existing_old_ids:
                        product['categories'].append(c)
                        existing_old_ids.add(c['old_id'])
            else:
                # tentar reconhecer reviews com regex (aceita sem indentação)
                m = REVIEW_RE.match(line)
                if m:
                    date_s, cust, rating, votes, helpful = m.groups()
                    try:
                        date_obj = datetime.strptime(date_s, '%Y-%m-%d').date()
                    except:
                        date_obj = None
                    product['reviews'].append({
                        'date': date_obj,
                        'customer': cust,
                        'rating': int(rating),
                        'votes': int(votes),
                        'helpful': int(helpful)
                    })
                # else: ignora outras linhas (ex: 'reviews: total: 8 downloaded: 8 avg rating: 4')
        # fim do loop
        if product:
            yield product
