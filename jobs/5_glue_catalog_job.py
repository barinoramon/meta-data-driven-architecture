import json
from framework.mdda_framework import BaseJob, ConfigLoader # Importa do framework

class GlueCatalogJob(BaseJob):
    """Job MDDA para atualizar metadados do catálogo (manifestos locais)."""
    def __init__(self):
        super().__init__(job_name="GlueCatalogJob")
        self.data_contract = None
        self.governance_manifest = None

    # Não precisa de Spark para manipular manifestos
    def _init_spark(self): pass
    def _stop_spark(self): pass

    def add_args(self, parser):
        parser.add_argument('--data_contract', required=True, help='Local path to 02-data-contract-*.json')
        parser.add_argument('--governance_manifest', required=True, help='Local path to 06-governance-manifest-*.json')
        parser.add_argument('--target_layer', required=True, choices=['bronze', 'silver', 'gold'], help='Layer whose catalog metadata is being updated')
        parser.add_argument('--execution_metrics_json', default='{}', help='JSON string with execution metrics (e.g., row count)')

    def run(self):
        self.log(self.job_name, "INFO", f"Iniciando atualização do catálogo para camada {self.args.target_layer}")
        self.data_contract = self.config_loader.load_json_local(self.args.data_contract)
        self.governance_manifest = self.config_loader.load_json_local(self.args.governance_manifest)
        try:
            execution_metrics = json.loads(self.args.execution_metrics_json)
        except json.JSONDecodeError:
            self.log(self.job_name, "WARNING", f"Não foi possível decodificar execution_metrics_json: {self.args.execution_metrics_json}")
            execution_metrics = {}

        # Identificar Tabela Alvo
        target_layer = self.args.target_layer
        pipeline_id = self.pipeline_def['metadata']['pipeline_id']
        domain = self.pipeline_def['metadata']['domain']
        table_name_suffix = pipeline_id.replace(f"{domain}_", "").replace("_v", "v")
        db_name = f"db_{domain}_{target_layer}"
        table_name = f"{domain}_{target_layer}_{table_name_suffix}"
        resource_id = f"{db_name}.{table_name}"
        self.log(self.job_name, "INFO", f"Atualizando manifesto para: {resource_id}")

        # Preparar Metadados
        params_to_update = {
            'last_updated_timestamp': datetime.now().isoformat(),
            'last_updated_by_job': self.job_name,
            'pipeline_version': self.pipeline_def.get('metadata', {}).get('version', 'unknown'),
            'data_contract_version': self.data_contract.get('contract', {}).get('version', 'unknown'),
            'governance_manifest_version': self.governance_manifest.get('metadata', {}).get('version', 'unknown'),
            'data_sensitivity': self.governance_manifest.get('data_classification', {}).get('sensitivity_level', 'internal'),
        }
        params_to_update.update(execution_metrics)

        column_comments = {
            col['name']: col.get('description', '')
            for col in self.data_contract.get('schema_specification', {}).get('structural', {}).get('columns', [])
            if col.get('description')
        }

        # Atualizar usando Catalog Manager
        try:
            # Atualiza parâmetros e comentários no manifesto existente
            self.catalog_manager.update_table_parameters(db_name, table_name, params_to_update)
            self.catalog_manager.update_column_comments(db_name, table_name, column_comments)
            self.log(self.job_name, "SUCCEEDED", {"updated_params": params_to_update})
        except Exception as e:
             self.log(self.job_name, "FAILED", {"error": f"Falha ao atualizar manifesto para {resource_id}: {str(e)}"})
             raise # Re-lança o erro para falhar o job


# --- Ponto de Entrada ---
if __name__ == "__main__":
    job = GlueCatalogJob()
    job.execute()
