# ==================================================
# Arquivo: 1_create_table_job.py
# Descrição: Job para criar/atualizar tabelas (manifestos locais).
# ==================================================
import sys
import json
import argparse
from pyspark.sql import SparkSession
from datetime import datetime # <<< IMPORTAÇÃO NECESSÁRIA
import os
import re

# Importa a classe base e utilitários do framework
try:
    from framework.mdda_framework import BaseJob, ConfigLoader
except ImportError:
    project_root_for_import = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, project_root_for_import)
    from framework.mdda_framework import BaseJob, ConfigLoader


class CreateTableJob(BaseJob):
    """Job MDDA para criar/atualizar definições de tabelas (manifestos locais)."""
    def __init__(self):
        super().__init__(job_name="CreateTableJob")
        self.data_contract = None

    def add_args(self, parser):
        parser.add_argument('--data_contract', required=True, help='Local path to 02-data-contract-*.json')

    def _get_local_table_definition(self, layer):
        config_pipeline = self.pipeline_def
        config_contract = self.data_contract
        pipeline_id = config_pipeline['metadata']['pipeline_id']
        domain = config_pipeline['metadata']['domain']
        table_name_suffix = re.sub(r'_v\d+', '', pipeline_id.replace(f"{domain}_", "")) + "v" + config_pipeline['metadata']['version'].split('.')[0]
        table_name = f"{domain}_{layer}_{table_name_suffix}"
        db_name = f"db_{domain}_{layer}"

        target_location = None
        for dest in config_pipeline.get('destinations', []):
            # Ajuste na heurística para ser mais robusta
            target_path_parts = dest.get('target', '').strip('/').split('/')
            if layer in target_path_parts and domain in target_path_parts and table_name in target_path_parts[-1]:
                 target_location = dest['target']
                 break
        if not target_location:
             target_location = f"./data/{layer}/{domain}/{table_name}/"
             self.log(self.job_name, "WARNING", f"Destino não encontrado explicitamente para {layer}, usando padrão: {target_location}")

        table_input = {
            'DatabaseName': db_name, 'Name': table_name,
            'Description': f"Tabela (simulada) da camada {layer} para {pipeline_id}",
            'StorageDescriptor': {'Columns': [], 'Location': target_location, 'InputFormat': 'Parquet', 'OutputFormat': 'Parquet', 'SerdeInfo': {'SerializationLibrary': 'parquet'}},
            'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE',
            'Parameters': {'classification': 'parquet', 'source_pipeline': pipeline_id, 'data_contract_id': config_pipeline['metadata']['data_contract_id'],
                           'created_by_job': self.job_name, 'creation_timestamp': datetime.now().isoformat(), 'is_simulated': 'true'}
        }
        schema_cols = config_contract.get('schema_specification', {}).get('structural', {}).get('columns', [])

        bronze_cols = ['order_id', 'product_id', 'quantity', 'unit_price_brl', 'order_timestamp', 'customer_email', 'status']
        silver_cols = bronze_cols + ['unit_price_usd', 'total_value_usd']
        gold_cols = ['product_id', 'order_date', 'total_quantity', 'total_aggregated_value_usd']

        cols_to_add_defs = []
        if layer == 'bronze':
            # Correção: Verifica se 'name' existe antes de usá-lo no filtro
            cols_to_add_defs = [col for col in schema_cols if col.get('name') and col['name'] in bronze_cols]
            cols_to_add_defs.append({'Name': '_ingestion_timestamp', 'Type': 'timestamp', 'Comment': 'Timestamp da ingestão'})
        elif layer == 'silver':
            # Correção: Verifica se 'name' existe antes de usá-lo no filtro
            cols_to_add_defs = [col for col in schema_cols if col.get('name') and col['name'] in silver_cols]
        elif layer == 'gold':
            # Correção: Verifica se 'name' existe antes de usá-lo no filtro
            cols_to_add_defs = [col for col in schema_cols if col.get('name') and col['name'] in gold_cols]
            table_input['PartitionKeys'] = [{'Name': 'order_date', 'Type': 'date'}]

        # Correção: Garante que todas as colunas em cols_to_add_defs tenham 'name' e 'data_type'
        final_columns = []
        for c in cols_to_add_defs:
            col_name = c.get('Name') # Usa 'Name' se adicionado manualmente, senão 'name' do JSON
            if not col_name: col_name = c.get('name')
            col_type = c.get('Type') # Usa 'Type' se adicionado manualmente, senão 'data_type'
            if not col_type: col_type = c.get('data_type')
            col_comment = c.get('Comment') # Usa 'Comment' se adicionado manualmente, senão 'description'
            if not col_comment: col_comment = c.get('description','')

            if col_name and col_type: # Só adiciona se tiver nome e tipo
                 final_columns.append({'Name': col_name, 'Type': col_type, 'Comment': col_comment})
            else:
                 self.log(self.job_name, "WARNING", f"Coluna ignorada na camada {layer} por falta de 'name' ou 'data_type': {c}")

        table_input['StorageDescriptor']['Columns'] = final_columns

        os.makedirs(target_location, exist_ok=True)
        return db_name, table_name, table_input

    def _simulate_create_or_update_glue_database(self, db_name):
        self.log(self.job_name, "INFO", f"SIMULATE: Verificando/Criando banco de dados '{db_name}'...")

    def run(self):
        """Lógica principal do job CreateTable."""
        self.log(self.job_name, "INFO", "Carregando contrato de dados...")
        self.data_contract = self.config_loader.load_json_local(self.args.data_contract)
        self.log(self.job_name, "INFO", "Contrato de dados carregado.")

        for layer in ['bronze', 'silver', 'gold']:
            self.log(self.job_name, "INFO", f"Processando camada: {layer.upper()}")
            db_name, table_name, table_definition = self._get_local_table_definition(layer)
            self._simulate_create_or_update_glue_database(db_name)
            self.catalog_manager.save_table_definition(db_name, table_name, table_definition)

# --- Ponto de Entrada ---
if __name__ == "__main__":
    job = CreateTableJob()
    job.execute()
