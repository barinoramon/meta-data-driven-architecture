import os

import datetime
from framework.mdda_framework import BaseJob, ConfigLoader


class GovernanceJob(BaseJob):
    """Job MDDA para aplicar políticas de governança (simulado)."""
    def __init__(self):
        super().__init__(job_name="GovernanceJob")
        self.governance_manifest = None

    def add_args(self, parser):
        parser.add_argument('--governance_manifest', required=True, help='Local path to 06-governance-manifest-*.json')

    def _simulate_apply_tags(self, resource_identifier, tags):
        if not tags: return
        self.log(self.job_name, "INFO", f"SIMULATE: Aplicando tags {tags} a {resource_identifier}")

    def _simulate_grant_permissions(self, principal, resource_identifier, permissions, masking_rules):
        self.log(self.job_name, "INFO", f"SIMULATE: Concedendo {permissions} a {principal} em {resource_identifier} com mascaramento: {masking_rules}")

    def run(self):
        self.log(self.job_name, "INFO", "Carregando manifesto de governança...")
        self.governance_manifest = self.config_loader.load_json_local(self.args.governance_manifest)
        self.log(self.job_name, "INFO", "Manifesto carregado.")

        pipeline_id = self.pipeline_def['metadata']['pipeline_id']
        domain = self.pipeline_def['metadata']['domain']
        table_name_suffix = pipeline_id.replace(f"{domain}_", "").replace("_v", "v")
        access_rules = self.governance_manifest.get('access_control', {}).get('rbac', [])

        for layer in ['bronze', 'silver', 'gold']:
            db_name = f"db_{domain}_{layer}"
            table_name = f"{domain}_{layer}_{table_name_suffix}"
            resource_id = f"{db_name}.{table_name}"
            self.log(self.job_name, "INFO", f"Aplicando governança para: {resource_id} (Camada: {layer})")

            # Tags
            classification_tags = self.governance_manifest.get('data_classification', {}).get('tags', [])
            sensitivity = self.governance_manifest.get('data_classification', {}).get('sensitivity_level', 'internal')
            tags_to_apply = {tag.lower(): "true" for tag in classification_tags}
            tags_to_apply['sensitivity'] = sensitivity; tags_to_apply['domain'] = domain; tags_to_apply['pipeline_id'] = pipeline_id
            self._simulate_apply_tags(resource_id, tags_to_apply)

            # Permissões RBAC
            for rule in access_rules:
                if layer in rule.get('applies_to_layers', ['bronze', 'silver', 'gold']):
                    principal_id = f"role:{rule['role']}"
                    permissions = [p.upper() for p in rule.get('permissions', [])]
                    masking = rule.get('masking_rules', [])
                    self._simulate_grant_permissions(principal_id, resource_id, permissions, masking)

            # Retenção (Informativo)
            retention = self.governance_manifest.get('retention_policies', {}).get(layer)
            if retention: self.log(self.job_name, "INFO", f"SIMULATE: Política de retenção para {layer}: {retention}")

        # Auditoria (Informativo)
        audit_config = self.governance_manifest.get('audit', {})
        if audit_config:
             self.log(self.job_name, "INFO", f"SIMULATE: Configuração de auditoria: {audit_config}")
             audit_log_path = audit_config.get('export_settings', {}).get('local_path')
             if audit_log_path: os.makedirs(audit_log_path, exist_ok=True)

# --- Ponto de Entrada ---
if __name__ == "__main__":
    job = GovernanceJob()
    job.execute()
