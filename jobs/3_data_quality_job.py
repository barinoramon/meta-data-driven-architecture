import os
import datetime
from framework.mdda_framework import BaseJob, DataQualityRunner # Importa do framework

class DataQualityJob(BaseJob):
    """Job MDDA para executar verificações de qualidade de dados."""
    def __init__(self):
        super().__init__(job_name="DataQualityJob")
        self.dq_runner = None

    def add_args(self, parser):
        parser.add_argument('--dq_rules', required=True, help='Local path to 04-data-quality-rules-*.json')
        parser.add_argument('--input_data_path', required=True, help='Local path to input data file/directory to be checked')
        parser.add_argument('--input_format', default='parquet', choices=['parquet', 'csv', 'json'], help='Format of the input data')
        parser.add_argument('--target_layer', required=True, choices=['bronze', 'silver', 'gold'], help='Layer being validated')
        parser.add_argument('--processing_date', default=datetime.now().strftime('%Y-%m-%d'), help='Date being processed (for context/logging)')
        parser.add_argument('--dq_output_path', default='./data/dq_results/', help='Directory for Data Quality results/flags')

    def _init_spark(self):
        """Sobrescreve para inicializar Spark e DQ Runner."""
        super()._init_spark()
        self.dq_runner = DataQualityRunner(
            self.spark, self.config_loader, self.pipeline_def,
            self.args.dq_rules, self.args.dq_output_path
        )

    def run(self):
        """Lógica principal do job DataQuality."""
        self.log(self.job_name, "INFO", f"Iniciando DQ para camada {self.args.target_layer} em {self.args.input_data_path}")

        # Carregar dados de entrada
        try:
            if self.args.input_format == 'parquet':
                input_df = self.spark.read.parquet(self.args.input_data_path)
            elif self.args.input_format == 'csv':
                input_df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(self.args.input_data_path)
            elif self.args.input_format == 'json':
                input_df = self.spark.read.json(self.args.input_data_path)
            else:
                raise ValueError(f"Formato de entrada não suportado: {self.args.input_format}")
            read_count = input_df.count()
            self.log(self.job_name, "INFO", f"Registros lidos para DQ: {read_count}")
            if read_count == 0:
                 self.log(self.job_name, "WARNING", "Nenhum registro para verificar DQ.")
                 flag_dir = os.path.join(self.args.dq_output_path, self.pipeline_id, self.args.target_layer); os.makedirs(flag_dir, exist_ok=True)
                 flag_file_path = os.path.join(flag_dir, f"{self.args.processing_date}_PASS.flag"); open(flag_file_path, 'a').close()
                 return # Termina com sucesso
        except Exception as e:
             if "Path does not exist" in str(e):
                 error_msg = f"Diretório de dados para DQ não encontrado: {self.args.input_data_path}"
                 self.log(self.job_name, "FAILED", {"error": error_msg})
                 raise FileNotFoundError(error_msg)
             else:
                error_msg = f"Erro ao carregar dados para DQ de {self.args.input_data_path}: {str(e)}"
                self.log(self.job_name, "FAILED", {"error": error_msg})
                raise IOError(error_msg) from e

        # Executar verificações DQ
        # O método run_checks já lida com logging interno, flags e lança DataQualityError se deve abortar
        dq_status = self.dq_runner.run_checks(input_df, self.args.target_layer, self.args.processing_date)
        self.log(self.job_name, "INFO", f"Verificação DQ concluída para camada {self.args.target_layer} com status: {dq_status}")


# --- Ponto de Entrada ---
if __name__ == "__main__":
    job = DataQualityJob()
    job.execute()
