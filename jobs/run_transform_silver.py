import sys
import json
import argparse
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import os
import re

# Importa do framework
from framework.mdda_framework import BaseJob, ConfigLoader, LocalCatalogManager, TransformationRunner

class TransformSilverJob(BaseJob):
    """Job MDDA para transformação de dados Bronze -> Silver."""
    def __init__(self):
        super().__init__(job_name="TransformSilverJob")
        self.transformation_runner = None

    def add_args(self, parser):
        """Adiciona argumentos específicos."""
        parser.add_argument('--template_dir', default='./configs/templates/', help='Directory containing transformation templates JSON files')
        parser.add_argument('--processing_date', default=datetime.now().strftime('%Y-%m-%d'), help='Date to process (YYYY-MM-DD)')
        parser.add_argument('--dq_output_path', default='./data/dq_results/', help='Directory for Data Quality results/flags')
        parser.add_argument('--skip_dq_check', action='store_true', help='Skip checking the DQ flag before running')


    def _init_spark(self):
        """Sobrescreve para inicializar Spark e Transformation Runner."""
        super()._init_spark()
        self.transformation_runner = TransformationRunner(
            self.spark, self.config_loader, self.args.template_dir
        )

    def _check_dq_status(self):
        """Verifica o flag de resultado do job DQ anterior."""
        if self.args.skip_dq_check:
            self.log(self.job_name, "WARNING", "Verificação de flag DQ pulada (--skip_dq_check).")
            return

        pipeline_id = self.pipeline_def['metadata']['pipeline_id']
        target_layer = 'silver' # Verifica o resultado do DQ da Silver
        processing_date = self.args.processing_date
        dq_output_path = self.args.dq_output_path

        dq_fail_flag_file = os.path.join(dq_output_path, pipeline_id, target_layer, f"{processing_date}_ERROR.flag")
        dq_job_fail_flag_file = os.path.join(dq_output_path, pipeline_id, target_layer, f"{processing_date}_JOB_FAIL.flag")

        if os.path.exists(dq_fail_flag_file) or os.path.exists(dq_job_fail_flag_file):
            flag_found = dq_fail_flag_file if os.path.exists(dq_fail_flag_file) else dq_job_fail_flag_file
            error_msg = f"Flag de falha de Data Quality encontrado: {flag_found}. Abortando."
            details = {"flag_file": flag_found}
            try:
                 with open(flag_found, 'r') as f_dq: details["dq_results"] = f_dq.read()
            except Exception: pass
            self.log(self.job_name, "FAILED", details)
            raise Exception(error_msg) # Lança exceção para parar o job
        else:
            dq_pass_flag_file = os.path.join(dq_output_path, pipeline_id, target_layer, f"{processing_date}_PASS.flag")
            dq_warn_flag_file = os.path.join(dq_output_path, pipeline_id, target_layer, f"{processing_date}_WARNING.flag")
            if not os.path.exists(dq_pass_flag_file) and not os.path.exists(dq_warn_flag_file):
                 warning_msg = f"Flag de sucesso/warning do DQ não encontrado para {processing_date}. Verifique se o job DQ rodou."
                 self.log(self.job_name, "WARNING", {"message": warning_msg})
                 # Considerar falhar aqui se o DQ for obrigatório
                 # raise FileNotFoundError(warning_msg)
            else:
                self.log(self.job_name, "INFO", "Verificação de Data Quality: Flag de sucesso/warning encontrado.")

    def run(self):
        """Lógica principal do job TransformSilver."""
        processing_dt = datetime.strptime(self.args.processing_date, '%Y-%m-%d')
        self.log(self.job_name, "STARTING", {"processing_date": self.args.processing_date})

        # Verificar Status DQ antes de prosseguir
        self._check_dq_status()

        # Determinar Caminhos
        domain = self.pipeline_def['metadata']['domain']
        pipeline_id = self.pipeline_def['metadata']['pipeline_id']
        table_name_suffix = pipeline_id.replace(f"{domain}_", "").replace("_v", "v")

        bronze_db = f"db_{domain}_bronze"
        bronze_table = f"{domain}_bronze_{table_name_suffix}"
        try:
            bronze_location = self.catalog_manager.get_table_location(bronze_db, bronze_table)
        except Exception as e:
             self.log(self.job_name, "FAILED", {"error": f"Falha ao obter location da tabela Bronze: {str(e)}"})
             raise
        bronze_partition_path = f"year={processing_dt.year}/month={processing_dt.month:02d}/day={processing_dt.day:02d}"
        bronze_read_path = os.path.join(bronze_location, bronze_partition_path)

        silver_db = f"db_{domain}_silver"
        silver_table = f"{domain}_silver_{table_name_suffix}"
        try:
            silver_write_path = self.catalog_manager.get_table_location(silver_db, silver_table)
        except Exception as e:
             self.log(self.job_name, "FAILED", {"error": f"Falha ao obter location da tabela Silver: {str(e)}"})
             raise

        self.log(self.job_name, "INFO", f"Lendo dados da Bronze de: {bronze_read_path}")
        self.log(self.job_name, "INFO", f"Escrevendo dados na Silver em: {silver_write_path}")

        # Ler Dados da Bronze
        try:
            df = self.spark.read.parquet(bronze_read_path)
            read_count = df.count()
            self.log(self.job_name, "INFO", f"Registros lidos da Bronze: {read_count}")
            if read_count == 0:
                self.log(self.job_name, "WARNING", "Nenhum registro na Bronze para processar.")
                self.log(self.job_name, "SUCCEEDED", {"message": "No input data"})
                return
        except Exception as e:
             if "Path does not exist" in str(e):
                 error_msg = f"Diretório de origem Bronze não encontrado: {bronze_read_path}"
                 self.log(self.job_name, "FAILED", {"error": error_msg})
                 raise FileNotFoundError(error_msg)
             else:
                error_msg = f"Erro ao ler dados da Bronze {bronze_read_path}: {str(e)}"
                self.log(self.job_name, "FAILED", {"error": error_msg})
                raise IOError(error_msg) from e

        # Aplicar Transformações Silver
        df_transformed = df
        transformations = self.pipeline_def['processing']['transformations']
        silver_transforms = [t for t in transformations if t.get("target_layer_step", "silver") == "silver"]
        self.log(self.job_name, "INFO", f"Aplicando {len(silver_transforms)} transformações Silver...")
        for transform in silver_transforms:
            template_id = transform['template_id']
            parameters = transform['parameters']
            df_transformed = self.transformation_runner.apply(df_transformed, template_id, parameters)

        self.log(self.job_name, "INFO", "Transformações da camada Silver aplicadas.")
        df_transformed.cache()
        write_count = df_transformed.count()

        # Escrever Dados na Silver
        self.log(self.job_name, "INFO", f"Escrevendo {write_count} registros transformados para {silver_write_path}")
        # Assume que Silver não é particionada ou sobrescrevemos tudo
        df_transformed.write.mode("overwrite").format("parquet").save(silver_write_path)
        df_transformed.unpersist()
        self.log(self.job_name, "INFO", f"Dados transformados com sucesso na camada Silver. Linhas escritas: {write_count}")
        # Log SUCCEEDED é feito automaticamente pela BaseJob

# --- Ponto de Entrada ---
if __name__ == "__main__":
    job = TransformSilverJob()
    job.execute()
