import json
from framework.mdda_framework import BaseJob, Logger # Importa do framework

class LogPipelineStepJob(BaseJob):
    """Job MDDA (utilitário) para registrar um passo específico do pipeline."""
    def __init__(self):
        # O nome real do job/step virá dos argumentos
        super().__init__(job_name="LogPipelineStepJob")

    # Não precisa de Spark
    def _init_spark(self): pass
    def _stop_spark(self): pass

    def add_args(self, parser):
        """Argumentos necessários para registrar um log."""
        parser.add_argument('--step_name', required=True, help='Nome do step/job sendo logado')
        parser.add_argument('--status', required=True, choices=['STARTING', 'SUCCEEDED', 'FAILED', 'INFO', 'WARNING'], help='Status do step')
        parser.add_argument('--log_details', required=True, help='Detalhes em formato JSON string ou mensagem simples')
        # --pipeline_def, --orchestration_config, --execution_id são herdados/parseados por BaseJob

    def run(self):
        """Lógica principal: apenas chama o método log da classe base."""
        # O logger já foi configurado em _parse_and_load_args da BaseJob
        if self.logger:
             # Tenta decodificar detalhes como JSON, senão usa como string
             try: details = json.loads(self.args.log_details)
             except json.JSONDecodeError: details = {"message": self.args.log_details}
             # Chama o método log herdado (que usa o self.logger configurado)
             self.log(self.args.step_name, self.args.status, details)
        else:
             print(f"ERRO: Logger não inicializado no LogPipelineStepJob. Verifique as configurações.", file=sys.stderr)
             # Log fallback (já acontece no BaseJob.log se logger for None)
             super().log(self.args.step_name, self.args.status, self.args.log_details)


# --- Ponto de Entrada ---
if __name__ == "__main__":
    # Este script é geralmente chamado por outros processos, não diretamente como um job completo
    job = LogPipelineStepJob()
    # Chama execute() para parsing de args e execução do run()
    job.execute()
