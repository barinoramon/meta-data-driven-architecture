import sys
import json
import argparse
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import os
import re

# Importa a classe base e utilitários do framework
from framework.mdda_framework import BaseJob, ConfigLoader, LocalCatalogManager

class IngestBronzeJob(BaseJob):
    """Job MDDA para ingestão de dados na camada Bronze."""
    def __init__(self):
        super().__init__(job_name="IngestBronzeJob")

    def add_args(self, parser):
        """Adiciona argumentos específicos para este job."""
        parser.add_argument('--processing_date', default=datetime.now().strftime('%Y-%m-%d'), help='Date to process (YYYY-MM-DD)')

    def run(self):
        """Lógica principal do job de ingestão Bronze."""
        processing_dt = datetime.strptime(self.args.processing_date, '%Y-%m-%d')
        self.log(self.job_name, "STARTING", {"processing_date": self.args.processing_date})

        # Carrega configurações específicas da fonte
        source_conf = self.pipeline_def['source_config']['bronze']
        source_location_template = source_conf['location']
        source_path = source_location_template.format(
            yyyy=processing_dt.year, MM=f"{processing_dt.month:02d}", dd=f"{processing_dt.day:02d}"
        )
        source_format = source_conf['format']
        self.log(self.job_name, "INFO", f"Lendo dados ({source_format}) de: {source_path}")

        # Ler Dados da Landing
        try:
            if source_format == 'csv':
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            else:
                # Adicionar suporte a outros formatos se necessário
                raise NotImplementedError(f"Formato de origem '{source_format}' não suportado.")
            read_count = df.count()
            self.log(self.job_name, "INFO", f"Registros lidos da origem: {read_count}")
            if read_count == 0:
                self.log(self.job_name, "WARNING", "Nenhum registro encontrado na origem.")
                self.log(self.job_name, "SUCCEEDED", {"message": "No data to process"})
                return # Termina com sucesso se não há dados
        except Exception as e:
             if "Path does not exist" in str(e):
                 error_msg = f"Diretório de origem não encontrado: {source_path}"
                 self.log(self.job_name, "FAILED", {"error": error_msg})
                 raise FileNotFoundError(error_msg)
             else:
                error_msg = f"Erro ao ler dados da origem {source_path}: {str(e)}"
                self.log(self.job_name, "FAILED", {"error": error_msg})
                raise IOError(error_msg) from e

        # Adicionar Metadados de Ingestão
        ingestion_timestamp = datetime.now()
        df = df.withColumn("_ingestion_timestamp", F.lit(ingestion_timestamp))

        # Determinar Destino Bronze usando Catalog Manager
        domain = self.pipeline_def['metadata']['domain']
        pipeline_id = self.pipeline_def['metadata']['pipeline_id']
        table_name_suffix = pipeline_id.replace(f"{domain}_", "").replace("_v", "v")
        bronze_db = f"db_{domain}_bronze"
        bronze_table = f"{domain}_bronze_{table_name_suffix}"
        try:
            bronze_target_location = self.catalog_manager.get_table_location(bronze_db, bronze_table)
        except Exception as e:
             self.log(self.job_name, "FAILED", {"error": f"Falha ao obter location da tabela Bronze: {str(e)}"})
             raise

        bronze_partition_path = f"year={processing_dt.year}/month={processing_dt.month:02d}/day={processing_dt.day:02d}"
        bronze_write_path = os.path.join(bronze_target_location, bronze_partition_path)
        self.log(self.job_name, "INFO", f"Escrevendo dados na Bronze (Parquet) em: {bronze_write_path}")

        # Escrever na Bronze
        df.write.mode("overwrite").format("parquet").save(bronze_write_path)
        # Verifica contagem após escrita para log
        try:
            written_count = self.spark.read.parquet(bronze_write_path).count()
        except Exception: # Pode falhar se o diretório estiver vazio após filtro implícito ou erro
            written_count = 0
        self.log(self.job_name, "INFO", f"{written_count} Registros escritos na Bronze.")
        # Log SUCCEEDED é feito automaticamente pela classe BaseJob

# --- Ponto de Entrada ---
if __name__ == "__main__":
    job = IngestBronzeJob()
    job.execute()