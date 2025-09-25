"""
Este módulo contém as funções para analisar (fazer o "parsing") do ficheiro de dados `amazon-meta.txt`, que tem um formato de texto semi-estruturado.

As funções foram desenhadas para serem eficientes em memória e robustas a pequenas inconsistências no formato do ficheiro.
"""

import re
from datetime import datetime

REVIEW_RE = re.compile(r"""
    ^\s* # possível espaço no começo da linha
    (?P<date>\d{4}-\d{1,2}-\d{1,2})    # data no formato YYYY-MM-DD
    \s+
    cutomer:?                           # palavra "cutomer" (possível typo: "customer"), com ":" opcional
    \s*
    (?P<customer>[^\s]+)                # nome do cliente (não espaços)
    \s+
    rating:\s*(?P<rating>\d+)           # rating
    \s+
    votes:\s*(?P<votes>\d+)             # número de votos
    \s+
    helpful:\s*(?P<helpful>\d+)         # número de votos úteis
""", re.IGNORECASE | re.VERBOSE)


# Regex para encontrar as partes de uma categoria, como: |Nome[ID]|
CAT_PART_RE = re.compile(r"""
    \|              # começa com um pipe literal "|"
    ([^|\[]+)       # captura um ou mais caracteres que nao seja "|" nem "["
    \[              # abre colchete "["
    (\d+)           # captura um número (um ou mais dígitos)
    \]              # fecha colchete "]"
    """, re.VERBOSE)

# Regex para a linha de resumo das reviews, ex: "reviews: total: 8 downloaded: 8 avg rating: 4"
REVIEW_SUMMARY_RE = re.compile(r"""
    reviews:
    \s+
    total:\s+(?P<total>\d+)
    \s+
    downloaded:\s+(?P<downloaded>\d+)
    \s+
    avg\srating:\s+(?P<avg_rating>[0-9.]+)
""", re.IGNORECASE | re.VERBOSE)


def extract_all_categories(path):
    """
    Faz uma primeira leitura rápida do ficheiro, focada apenas em extrair todas as
    categorias, os seus IDs antigos e a relação de hierarquia (pai/filho).
    """
    categories = {}  # Dicionário para guardar as categorias encontradas
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        for raw_line in f:
            line = raw_line.strip()
            # Só processamos linhas que parecem ser de categorias
            if '|' in line and '[' in line:
                parts = CAT_PART_RE.findall(line)
                parent_old_id = None
                for name, id_str in parts:
                    try:
                        old_id = int(id_str)
                    except ValueError:
                        continue  # Ignora se o ID não for um número
                    
                    name = name.strip()
                    # Adiciona a categoria ao nosso dicionário apenas se for a primeira vez que a vemos
                    if old_id not in categories:
                        categories[old_id] = {"name": name, "parent_old_id": parent_old_id}
                    
                    # O ID atual será o pai da próxima categoria na mesma linha
                    parent_old_id = old_id
    return categories


def _parse_category_line_to_list(line):
    """
    Função auxiliar para transformar uma linha de texto de categorias numa lista estruturada.
    """
    parts = CAT_PART_RE.findall(line)
    structured_cats = []
    parent_id = None
    for name, id_str in parts:
        try:
            old_id = int(id_str)
        except ValueError:
            continue
        
        name = name.strip()
        structured_cats.append({"old_id": old_id, "name": name, "parent_old_id": parent_id})
        parent_id = old_id
    return structured_cats


def parse_snap(path):
    """
    Função principal de parsing. Lê o ficheiro de dados produto a produto.
    Esta função é um "gerador" (usa 'yield'), o que significa que não carrega
    o ficheiro todo para a memória, tornando o processo muito eficiente.
    """
    current_product = None

    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        for raw_line in f:
            line = raw_line.strip()
            line_lower = line.lower() # Normalizamos para minúsculas para evitar problemas com 'Title' vs 'title'

            # Se a linha começa com 'Id:', sabemos que um novo produto começou
            if line_lower.startswith('id:'):
                if current_product:
                    yield current_product # Devolve o produto anterior completo
                
                # Inicia um novo dicionário para o produto atual
                current_product = {
                    'id': int(line.split(':', 1)[1].strip()), 'asin': None, 'title': None,
                    'group': None, 'salesrank': None, 'similar': [],
                    'categories': [], 'reviews': [],
                    'similar_count': 0, 'categories_count': 0,
                    'total': 0, 'downloaded': 0, 'avg_rating': None
                }
            elif current_product is None:
                continue # Ignora linhas antes do primeiro produto
            
            # Extrai os campos do produto, usando a linha em minúsculas para a verificação
            elif line_lower.startswith('asin:'):
                current_product['asin'] = line.split(':', 1)[1].strip()
            elif line_lower.startswith('title:'):
                current_product['title'] = line.split(':', 1)[1].strip()
            elif line_lower.startswith('group:'):
                current_product['group'] = line.split(':', 1)[1].strip()
            elif line_lower.startswith('salesrank:'):
                value_str = line.split(':', 1)[1].strip()
                current_product['salesrank'] = int(value_str) if value_str.isdigit() else None
            elif line_lower.startswith('similar:'):
                parts = line.split()
                if len(parts) > 1 and parts[1].isdigit():
                    current_product['similar_count'] = int(parts[1])
                if len(parts) > 2:
                    current_product['similar'] = parts[2:]
            
            # Extrai as categorias
            elif '|' in line:
                cats = _parse_category_line_to_list(line)
                current_product['categories_count'] += len(cats)
                current_product['categories'].extend(cats)
            
            elif line_lower.startswith('reviews: total:'):
                match = REVIEW_SUMMARY_RE.search(line)
                if match:
                    summary = match.groupdict()
                    current_product['total'] = int(summary['total'])
                    current_product['downloaded'] = int(summary['downloaded'])
                    current_product['avg_rating'] = float(summary['avg_rating'])

            # Se não for nenhum dos campos acima, tenta ver se é uma linha de avaliação
            else:
                match = REVIEW_RE.match(line)
                if match:
                    date_str, customer_id, rating, votes, helpful = match.groups()
                    try:
                        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except ValueError:
                        date_obj = None
                    
                    current_product['reviews'].append({
                        'date': date_obj, 'customer': customer_id, 'rating': int(rating),
                        'votes': int(votes), 'helpful': int(helpful)
                    })
        
        # Devolve o último produto do ficheiro
        if current_product:
            yield current_product