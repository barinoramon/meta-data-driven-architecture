import sys
import json
import argparse
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import os
import re

# Importa do framework
from framework.mdda_framework import BaseJob, ConfigLoader, LocalCatalogManager, TransformationRunner

class TransformGoldJob(BaseJob):
    """Job MDDA para transformação de dados Silver -> Gold."""
    def __init__(self):
        super().__init__(job_name="TransformGoldJob")
        self.transformation_runner = None

    def add_args(self, parser):
        """Adiciona argumentos específicos."""
        parser.add_argument('--template_dir', default='./configs/templates/', help='Directory containing transformation templates JSON files')
        # Gold geralmente processa tudo da Silver, não precisa de data específica como input direto
        # parser.add_argument('--processing_date', default=datetime.now().strftime('%Y-%m-%d'), help='Date context for logging (YYYY-MM-DD)')

    def _init_spark(self):
        """Sobrescreve para inicializar Spark e Transformation Runner."""
        super()._init_spark()
        self.transformation_runner = TransformationRunner(
            self.spark, self.config_loader, self.args.template_dir
        )

    def run(self):
        """Lógica principal do job TransformGold."""
        self.log(self.job_name, "STARTING", {})

        # Determinar Caminhos
        domain = self.pipeline_def['metadata']['domain']
        pipeline_id = self.pipeline_def['metadata']['pipeline_id']
        table_name_suffix = pipeline_id.replace(f"{domain}_", "").replace("_v", "v")

        silver_db = f"db_{domain}_silver"
        silver_table = f"{domain}_silver_{table_name_suffix}"
        try:
            silver_read_path = self.catalog_manager.get_table_location(silver_db, silver_table)
        except Exception as e:
             self.log(self.job_name, "FAILED", {"error": f"Falha ao obter location da tabela Silver: {str(e)}"})
             raise

        gold_db = f"db_{domain}_gold"
        gold_table = f"{domain}_gold_{table_name_suffix}"
        try:
            gold_write_path_base = self.catalog_manager.get_table_location(gold_db, gold_table)
        except Exception as e:
             self.log(self.job_name, "FAILED", {"error": f"Falha ao obter location da tabela Gold: {str(e)}"})
             raise

        self.log(self.job_name, "INFO", f"Lendo dados da Silver de: {silver_read_path}")
        self.log(self.job_name, "INFO", f"Escrevendo dados na Gold em: {gold_write_path_base} (particionado por order_date)")

        # Ler Dados da Silver
        try:
            df_silver = self.spark.read.parquet(silver_read_path)
            read_count = df_silver.count()
            self.log(self.job_name, "INFO", f"Registros lidos da Silver: {read_count}")
            if read_count == 0:
                 self.log(self.job_name, "WARNING", "Nenhum registro na Silver para processar.")
                 self.log(self.job_name, "SUCCEEDED", {"message": "No input data"})
                 return
        except Exception as e:
             if "Path does not exist" in str(e):
                 error_msg = f"Diretório de origem Silver não encontrado: {silver_read_path}"
                 self.log(self.job_name, "FAILED", {"error": error_msg})
                 raise FileNotFoundError(error_msg)
             else:
                error_msg = f"Erro ao ler dados da Silver {silver_read_path}: {str(e)}"
                self.log(self.job_name, "FAILED", {"error": error_msg})
                raise IOError(error_msg) from e

        # Aplicar Transformação Gold (Agregação)
        df_gold = df_silver
        transformations = self.pipeline_def['processing']['transformations']
        gold_transforms = [t for t in transformations if t.get("target_layer_step") == "gold"]
        if not gold_transforms:
             error_msg = "Nenhuma transformação definida para a camada Gold no pipeline definition."
             self.log(self.job_name, "FAILED", {"error": error_msg})
             raise ValueError(error_msg)

        self.log(self.job_name, "INFO", f"Aplicando {len(gold_transforms)} transformações Gold...")
        for transform in gold_transforms: # Normalmente haverá apenas uma para agregação Gold
            template_id = transform['template_id']
            parameters = transform['parameters']
            # Passa o DataFrame atual (df_gold) para a transformação
            df_gold = self.transformation_runner.apply(df_gold, template_id, parameters)

        self.log(self.job_name, "INFO", "Transformações da camada Gold aplicadas.")
        df_gold.cache()
        write_count = df_gold.count()

        # Escrever Dados na Gold (Particionado)
        self.log(self.job_name, "INFO", f"Escrevendo {write_count} registros agregados para {gold_write_path_base}")
        if "order_date" not in df_gold.columns:
             error_msg = "Erro: Coluna de partição 'order_date' não encontrada no DataFrame Gold final."
             self.log(self.job_name, "FAILED", {"error": error_msg})
             raise ValueError(error_msg)

        # Overwrite mode substituirá partições existentes se elas forem regeradas
        df_gold.write.mode("overwrite").format("parquet").partitionBy("order_date").save(gold_write_path_base)
        df_gold.unpersist()
        self.log(self.job_name, "INFO", f"Dados agregados com sucesso na camada Gold. Linhas escritas: {write_count}")
        # Log SUCCEEDED é feito automaticamente pela BaseJob

# --- Ponto de Entrada ---
if __name__ == "__main__":
    job = TransformGoldJob()
    job.execute()
