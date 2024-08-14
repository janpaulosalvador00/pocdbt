# dags/gera_schema_yml.py

import yaml
import os

# Caminho para o diretório de modelos
models_dir = 'dags/dbt/models/3_trusted/'

# Caminho para o arquivo sources.yml
sources_file = 'dags/dbt/models/2_raw/sources.yml'

def get_tables_from_sources(sources_file):
    with open(sources_file, 'r') as file:
        sources = yaml.safe_load(file)
    
    tables = []
    for source in sources.get('sources', []):
        for table in source.get('tables', []):
            tables.append(table['name'])
    
    return tables

def generate_schema_yml(tables):
    schema_content = {
        'version': 2,
        'models': []
    }

    for table in tables:
        model_entry = {
            'name': f'{table}_transformed',
            'description': f'Modelo transformado da tabela {table}.',
            'columns': [
                {'name': 'id', 'description': 'Identificador único.', 'tests': ['not_null', 'valid_id']},
                {'name': 'data', 'description': 'Coluna de data.', 'tests': ['not_null', 'valid_date']},
                {'name': 'nome', 'description': 'Nome formatado.'}
            ]
        }
        schema_content['models'].append(model_entry)
    
    return schema_content

def save_schema_yml(schema_content, filename='dags/dbt/models/3_trusted/schema.yml'):
    with open(filename, 'w') as file:
        yaml.dump(schema_content, file, sort_keys=False)

def run_script():
    tables = get_tables_from_sources(sources_file)
    schema_content = generate_schema_yml(tables)
    save_schema_yml(schema_content)
