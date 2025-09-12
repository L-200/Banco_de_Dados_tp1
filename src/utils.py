# src/utils.py
import re
from datetime import datetime

REVIEW_RE = re.compile(
    r'(\d{4}-\d{1,2}-\d{1,2})\s+(?:cutomer|customer):\s*(\S+)\s+rating:\s*(\d+)\s+votes:\s*(\d+)\s+helpful:\s*(\d+)'
)

def parse_snap(path):
    """
    Gera dicionários de produto a partir do arquivo SNAP.
    Cada produto:
    {
      'id': int, 'asin': str, 'title': str, 'group': str, 'salesrank': int,
      'similar': [asins], 'categories': [category_name,...], 'reviews': [{date,customer,rating,votes,helpful},...]
    }
    """
    product = None
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        for raw in f:
            line = raw.rstrip('\n')
            if line.startswith('Id:'):
                if product:
                    yield product
                product = {
                    'id': int(line.split(':',1)[1].strip()),
                    'asin': None, 'title': None, 'group': None, 'salesrank': None,
                    'similar': [], 'categories': [], 'reviews': []
                }
            elif line.startswith('ASIN:'):
                product['asin'] = line.split(':',1)[1].strip()
            elif line.startswith('title:'):
                product['title'] = line.split(':',1)[1].strip()
            elif line.startswith('group:'):
                product['group'] = line.split(':',1)[1].strip()
            elif line.startswith('salesrank:'):
                v = line.split(':',1)[1].strip()
                try:
                    product['salesrank'] = int(v)
                except:
                    product['salesrank'] = None
            elif line.startswith('similar:'):
                parts = line.split()
                if len(parts) > 2:
                    product['similar'] = parts[2:]
            elif line.startswith('categories:'):
                # número de categorias vem aqui; as linhas com '|' são as categories reais (tratadas abaixo)
                pass
            elif line.strip().startswith('|'):  # linha de categorias
                stripped = line.strip()
                # separa por '|' e remove índices [..]
                parts = [p for p in stripped.split('|') if p]
                for p in parts:
                    name = re.sub(r'\[.*?\]', '', p).strip()
                    if name and name not in product['categories']:
                        product['categories'].append(name)
            elif line.startswith('    ') or line.startswith('\t'):  # review lines (indentadas)
                stripped = line.strip()
                m = REVIEW_RE.match(stripped)
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
                # else: ignora linhas estranhas
        if product:
            yield product
