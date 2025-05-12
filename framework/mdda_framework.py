# ==================================================
# Arquivo: mdda_framework.py
# Descrição: Framework base com classes e utilitários para jobs MDDA modulares.
# ==================================================
import sys
import json
import argparse
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import os
import re
import uuid
from abc import ABC, abstractmethod

# --- Classe Utilitária para Configuração ---

class ConfigLoader:
    """Classe para carregar configurações JSON, removendo comentários."""
    @staticmethod
    def load_json_local(file_path):
        """Carrega um arquivo JSON local, tratando erros comuns."""
        if not os.path.exists(file_path):
            print(f"Erro Crítico: Arquivo de configuração não encontrado em {file_path}")
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # --- REMOVIDO PARA TESTE ---
                # content = f.read()
                # content = re.sub(r'//.*?\n', '\n', content)
                # content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
                # content = re.sub(r',\s*([\}\]])', r'\1', content)
                # return json.loads(content)
                # --- FIM DA REMOÇÃO ---

                # --- VERSÃO SIMPLES ---
                return json.load(f) # Carrega diretamente do stream do arquivo
                # --- FIM DA VERSÃO SIMPLES ---
        except json.JSONDecodeError as e:
            print(f"Erro Crítico ao decodificar JSON de {file_path}: {e}")
            print(f"  Linha: {e.lineno}, Coluna: {e.colno}, Mensagem: {e.msg}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f_debug:
                    lines = f_debug.readlines()
                    start = max(0, e.lineno - 3)
                    end = min(len(lines), e.lineno + 2)
                    print("  Conteúdo próximo ao erro:")
                    for i in range(start, end):
                        print(f"    {i+1}: {lines[i].strip()}")
            except Exception as read_err:
                 print(f"  Não foi possível ler o conteúdo para depuração: {read_err}")
            raise
        except Exception as e:
            print(f"Erro Crítico inesperado ao carregar JSON de {file_path}: {e}")
            raise

# --- Classe para Gerenciar Catálogo Local Simulado ---
class LocalCatalogManager:
    """Gerencia manifestos de tabelas no catálogo local simulado."""
    def __init__(self, manifest_base_dir="./local_catalog_manifest"):
        self.manifest_base_dir = manifest_base_dir
        os.makedirs(self.manifest_base_dir, exist_ok=True)
        # print(f"INFO: Gerenciador de Catálogo Local usando diretório: {self.manifest_base_dir}")

    def _get_manifest_path(self, db_name, table_name):
        return os.path.join(self.manifest_base_dir, f"{db_name}.{table_name}.json")

    def save_table_definition(self, db_name, table_name, table_definition):
        """Salva ou atualiza a definição da tabela no manifesto local."""
        manifest_path = self._get_manifest_path(db_name, table_name)
        # print(f"INFO: Salvando/Atualizando manifesto da tabela '{db_name}.{table_name}' em {manifest_path}")
        try:
            os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(table_definition, f, indent=2)
            # print(f"  Manifesto salvo com sucesso.")
        except Exception as e:
            print(f"AVISO: Falha ao salvar manifesto local da tabela '{db_name}.{table_name}': {e}")

    def get_table_definition(self, db_name, table_name):
        """Carrega a definição da tabela do manifesto local."""
        manifest_path = self._get_manifest_path(db_name, table_name)
        try:
            return ConfigLoader.load_json_local(manifest_path)
        except FileNotFoundError:
             # print(f"Erro: Manifesto da tabela não encontrado em {manifest_path}.") # Logado pelo ConfigLoader
             raise
        except Exception as e:
            print(f"Erro ao ler definição do manifesto {manifest_path}: {e}")
            raise

    def get_table_location(self, db_name, table_name):
        """Obtém o caminho (location) da tabela a partir do manifesto."""
        try:
            table_def = self.get_table_definition(db_name, table_name)
            location = table_def.get('StorageDescriptor', {}).get('Location')
            if not location:
                 raise ValueError(f"Location não encontrado no manifesto para '{db_name}.{table_name}'")
            return location
        except Exception as e:
            print(f"Erro ao obter location para '{db_name}.{table_name}': {e}")
            raise

    def update_table_parameters(self, db_name, table_name, params_to_update):
        """Adiciona ou atualiza parâmetros no manifesto da tabela."""
        # print(f"INFO: Atualizando parâmetros para tabela '{db_name}.{table_name}'...")
        try:
            current_manifest = self.get_table_definition(db_name, table_name)
            if 'Parameters' not in current_manifest: current_manifest['Parameters'] = {}
            current_manifest['Parameters'].update(params_to_update)
            self.save_table_definition(db_name, table_name, current_manifest)
            # print(f"  Parâmetros atualizados: {params_to_update}")
        except Exception as e:
            print(f"ERRO ao atualizar parâmetros para '{db_name}.{table_name}': {e}")

    def update_column_comments(self, db_name, table_name, column_comments):
        """Atualiza os comentários das colunas no manifesto da tabela."""
        if not column_comments: return
        # print(f"INFO: Atualizando comentários de colunas para tabela '{db_name}.{table_name}'...")
        try:
            current_manifest = self.get_table_definition(db_name, table_name)
            if 'StorageDescriptor' not in current_manifest: current_manifest['StorageDescriptor'] = {'Columns': []}
            if 'Columns' not in current_manifest['StorageDescriptor']: current_manifest['StorageDescriptor']['Columns'] = []

            cols_dict = {col.get('Name'): col for col in current_manifest['StorageDescriptor']['Columns']}
            updated_comments = False
            for col_name, comment in column_comments.items():
                if col_name in cols_dict:
                    if cols_dict[col_name].get('Comment') != comment:
                        cols_dict[col_name]['Comment'] = comment
                        updated_comments = True
                        # print(f"    - Comentário atualizado para '{col_name}'")
            if updated_comments:
                self.save_table_definition(db_name, table_name, current_manifest)
            # else:
                # print("    - Nenhum comentário de coluna precisou ser atualizado.")
        except Exception as e:
            print(f"ERRO ao atualizar comentários para '{db_name}.{table_name}': {e}")

# --- Classe para Logging Estruturado ---
class Logger:
    """Classe para gerar logs estruturados do pipeline."""
    def __init__(self, pipeline_def, orchestration_config, execution_id, log_destination_path):
        self.pipeline_def = pipeline_def
        self.orchestration_config = orchestration_config
        self.execution_id = execution_id
        self.log_base_path = log_destination_path
        self.pipeline_id = self.pipeline_def['metadata']['pipeline_id']
        self.domain = self.pipeline_def['metadata']['domain']
        self.environment = self.orchestration_config.get('environment', {}).get('name', 'local')
        self.pipeline_version = self.pipeline_def.get('metadata', {}).get('version', 'unknown')
        # print(f"INFO: Logger inicializado para Execution ID: {self.execution_id}. Destino: {self.log_base_path}")

    def _get_log_file_key(self, timestamp):
        return os.path.join(
            self.domain, self.pipeline_id,
            f"year={timestamp.year}", f"month={timestamp.month:02d}", f"day={timestamp.day:02d}",
            f"{self.execution_id}.log"
        )

    def log_step(self, step_name, status, details):
        log_timestamp = datetime.now()
        serializable_details = {}
        if isinstance(details, dict):
            for k, v in details.items():
                try: json.dumps(v); serializable_details[k] = v
                except TypeError: serializable_details[k] = str(v)
        else:
            try: json.dumps(details); serializable_details = details
            except TypeError: serializable_details = {"message": str(details)}

        log_entry = {
            "log_id": str(uuid.uuid4()), "pipeline_id": self.pipeline_id, "execution_id": self.execution_id,
            "step_name": step_name, "domain": self.domain, "status": status.upper(),
            "timestamp_utc": log_timestamp.isoformat(), "details": serializable_details,
            "environment": self.environment, "pipeline_version": self.pipeline_version
        }
        log_file_key = self._get_log_file_key(log_timestamp)
        full_path = os.path.join(self.log_base_path, log_file_key)
        log_dir = os.path.dirname(full_path)
        try:
            os.makedirs(log_dir, exist_ok=True)
            with open(full_path, 'a', encoding='utf-8') as f:
                json.dump(log_entry, f); f.write('\n')
            print(f"LOG [{log_timestamp.isoformat()}] {self.pipeline_id}/{self.execution_id}/{step_name} - Status: {status.upper()} - Details: {details}")
        except Exception as e:
            print(f"ERRO AO ESCREVER LOG em {full_path}: {e}", file=sys.stderr)

# --- Classe para Executar Transformações ---
class TransformationRunner:
    """Executa transformações baseadas em templates."""
    def __init__(self, spark, config_loader, template_base_dir):
        self.spark = spark
        self.config_loader = config_loader
        self.template_base_dir = template_base_dir
        # print(f"INFO: Transformation Runner inicializado. Diretório de templates: {self.template_base_dir}")

    def _build_aggregation_sql(self, sql_template_structure, parameters):
        group_by_cols = parameters['group_by_columns']
        agg_specs = parameters['aggregation_specs']
        ts_col = parameters['timestamp_column_for_date']
        group_by_clause = ", ".join([f"`{col}`" for col in group_by_cols])
        aggregation_parts = [f"{spec['function'].upper()}(`{spec['source_column']}`) AS `{spec['target_column']}`" for spec in agg_specs]
        aggregation_clause = ", ".join(aggregation_parts)
        final_sql = sql_template_structure
        final_sql = final_sql.replace("{timestamp_column_for_date}", ts_col)
        final_sql = final_sql.replace("{group_by_clause}", group_by_clause)
        final_sql = final_sql.replace("{aggregation_clause}", aggregation_clause)
        return final_sql

    def apply(self, df, template_id, parameters):
        template_path = os.path.join(self.template_base_dir, f"{template_id}.json")
        # print(f"INFO: Aplicando template: {template_path} com parâmetros: {parameters}")
        try:
            template_def = self.config_loader.load_json_local(template_path)
            logic = template_def.get('logic', {})
            sql_template = logic.get('sql')
            sql_template_structure = logic.get('sql_template')

            if not sql_template and not sql_template_structure:
                 raise NotImplementedError(f"Lógica SQL ('sql' ou 'sql_template') não definida no template {template_path}")

            input_view_name = f"input_view_{template_def['template']['identifier'].replace('-','_')}_{uuid.uuid4().hex[:4]}"
            df.createOrReplaceTempView(input_view_name)

            if sql_template_structure: # Agregação
                final_sql = self._build_aggregation_sql(sql_template_structure, parameters)
                final_sql = final_sql.replace("{input_view_or_df}", input_view_name)
            else: # SQL Padrão
                final_sql = sql_template.replace("{input_view_or_df}", input_view_name)
                for key, value in parameters.items():
                    placeholder = "{" + key + "}"
                    replacement = f"{value}" if isinstance(value, str) else str(value)
                    final_sql = final_sql.replace(placeholder, replacement)
                    placeholder_backtick = "`{" + key + "}`"
                    replacement_backtick = f"`{value}`"
                    final_sql = final_sql.replace(placeholder_backtick, replacement_backtick)

            # print(f"  SQL a ser executado: {final_sql}")
            result_df = self.spark.sql(final_sql)
            self.spark.catalog.dropTempView(input_view_name)
            return result_df
        except Exception as e:
            print(f"Erro ao aplicar transformação do template {template_path}: {e}")
            try:
                if self.spark.catalog.tableExists(input_view_name):
                     self.spark.catalog.dropTempView(input_view_name)
            except Exception: pass
            raise

# --- Classe para Executar Data Quality ---
class DataQualityRunner:
    """Executa verificações de qualidade de dados."""
    def __init__(self, spark, config_loader, pipeline_def, dq_rules_path, dq_output_path):
        self.spark = spark
        self.config_loader = config_loader
        self.pipeline_def = pipeline_def
        self.dq_rules_path = dq_rules_path
        self.dq_output_path = dq_output_path
        self.pipeline_id = self.pipeline_def['metadata']['pipeline_id']
        self.quarantine_path = self.pipeline_def.get("error_handling", {}).get("quarantine_config", {}).get("path", f"./data/quarantine/{self.pipeline_id}/")
        # print(f"INFO: Data Quality Runner inicializado. Regras: {self.dq_rules_path}, Output: {self.dq_output_path}")

    def _execute_rule(self, df, rule):
        rule_id = rule['rule_id']; rule_type = rule['type']
        target_column = rule.get('target', {}).get('column')
        condition_sql = rule.get('condition', {}).get('sql')
        threshold_error = rule.get('thresholds', {}).get('error', 100.0)
        threshold_warning = rule.get('thresholds', {}).get('warning', 100.0)
        failing_df = None; status = "PASS"; success_percentage = 100.0; failing_records_count = 0

        # print(f"INFO: Executando regra DQ: {rule_id} ({rule_type}) - {rule['description']}")
        total_records = df.count()
        if total_records == 0: return "PASS", 100.0, 0, 0, None

        try:
            if condition_sql: failing_df = df.where(f"NOT ({condition_sql})")
            elif rule_type == 'completeness' and target_column: failing_df = df.where(F.col(target_column).isNull())
            elif rule_type == 'uniqueness' and target_column:
                duplicates_df = df.groupBy(target_column).count().where(F.col('count') > 1)
                failing_df = df.join(duplicates_df.select(target_column).distinct(), on=target_column, how='inner')
            else: return "SKIP", 100.0, total_records, 0, None

            if failing_df:
                failing_df.cache(); failing_records_count = failing_df.count()
                success_percentage = ((total_records - failing_records_count) / total_records) * 100

            # print(f"  Total: {total_records}, Falhas: {failing_records_count}, Sucesso: {success_percentage:.2f}%")
            if success_percentage < threshold_error: status = "ERROR"
            elif success_percentage < threshold_warning: status = "WARNING"
            # print(f"  Status da Regra: {status}")
        except Exception as e:
            print(f"  ERRO ao executar regra DQ {rule_id}: {e}")
            status = "EXECUTION_ERROR"; success_percentage = 0.0
            if failing_df and failing_df.is_cached: failing_df.unpersist(); failing_df = None
        return status, success_percentage, total_records, failing_records_count, failing_df

    def _trigger_action(self, action_type, rule, status, results, failing_df, notification_channels):
        rule_id = rule['rule_id']
        # print(f"  INFO: Disparando ação DQ '{action_type}' para regra {rule_id} (Status: {status})...")
        if action_type == "log": print(f"    LOG_ACTION: {results}") # Log já acontece no runner principal
        elif action_type == "notify_slack": print(f"    SIMULATE: Notificando Slack ({notification_channels.get('slack')}): Regra {rule_id} - Status {status}")
        elif action_type == "quarantine":
            if failing_df is not None and results['failing_records'] > 0:
                qt_path = os.path.join(self.quarantine_path, results['target_layer'], rule_id)
                # print(f"    INFO: Movendo {results['failing_records']} registros para quarentena: {qt_path}")
                try:
                    os.makedirs(qt_path, exist_ok=True); failing_df.write.mode("append").parquet(qt_path)
                    # print(f"      Registros em quarentena salvos.")
                except Exception as e: print(f"      ERRO ao salvar registros em quarentena: {e}")
                finally:
                    if failing_df.is_cached: failing_df.unpersist()
            # else: print("    INFO: Ação 'quarantine' sem registros falhos para mover.")
        elif action_type == "abort_pipeline": print(f"    INFO: Ação 'abort_pipeline' solicitada pela regra {rule_id}.")
        else: print(f"    AVISO: Ação DQ desconhecida: {action_type}")

    def run_checks(self, df, target_layer, processing_date):
        # print(f"INFO: Iniciando verificação DQ para camada '{target_layer}'...")
        overall_status = "PASS"; abort_requested = False; results_summary = []; all_failing_dfs = {}
        try:
            dq_rules_config = self.config_loader.load_json_local(self.dq_rules_path)
        except Exception as e: raise IOError(f"ERRO CRÍTICO: Não foi possível carregar regras DQ: {self.dq_rules_path}") from e

        applicable_rules = []
        if 'quality_profile' in dq_rules_config: profiles = [dq_rules_config['quality_profile']]
        elif isinstance(dq_rules_config.get('quality_profiles'), list): profiles = dq_rules_config['quality_profiles']
        else: profiles = [{'scope': [target_layer], 'rules': dq_rules_config.get('rules', [])}]
        for profile in profiles:
             if target_layer in profile.get('scope', [target_layer]): applicable_rules.extend(profile.get('rules', []))

        if not applicable_rules: print(f"INFO: Nenhuma regra DQ aplicável para '{target_layer}'."); return "PASS"

        # print(f"INFO: {len(applicable_rules)} regras DQ encontradas para '{target_layer}'.")
        global_config = dq_rules_config.get('global_config', {}); notification_channels = global_config.get('notification_channels', {})
        action_context = {'pipeline_id': self.pipeline_id, 'target_layer': target_layer, 'processing_date': processing_date}

        df.cache()
        try:
            for rule in applicable_rules:
                status, success_perc, total_rec, failing_rec, failing_df = self._execute_rule(df, rule)
                rule_result = {**action_context, 'rule_id': rule['rule_id'], 'status': status, 'success_percentage': success_perc, 'total_records': total_rec, 'failing_records': failing_rec}
                results_summary.append(rule_result)
                if failing_df: all_failing_dfs[rule['rule_id']] = failing_df
                if status in ["EXECUTION_ERROR", "ERROR"]: overall_status = "ERROR"
                elif status == "WARNING" and overall_status == "PASS": overall_status = "WARNING"
                actions_config = rule.get('actions', {}); actions_to_trigger = []
                if status == "ERROR": actions_to_trigger = actions_config.get('on_error', [])
                elif status == "WARNING": actions_to_trigger = actions_config.get('on_warning', [])
                current_failing_df = all_failing_dfs.get(rule['rule_id'])
                for action in actions_to_trigger:
                    self._trigger_action(action, rule, status, rule_result, notification_channels, current_failing_df)
                    if action == "abort_pipeline" and status == "ERROR": abort_requested = True
        finally:
             if df.is_cached: df.unpersist()
             for rule_id, fail_df in all_failing_dfs.items():
                  try:
                      if fail_df.is_cached: fail_df.unpersist()
                  except Exception: pass

        # print(f"\nResumo DQ para camada '{target_layer}': Status Geral: {overall_status}")
        flag_dir = os.path.join(self.dq_output_path, self.pipeline_id, target_layer); os.makedirs(flag_dir, exist_ok=True)
        flag_file_path = os.path.join(flag_dir, f"{processing_date}_{overall_status}.flag")
        try:
            with open(flag_file_path, 'w') as f_flag: json.dump(results_summary, f_flag, indent=2)
            # print(f"INFO: Arquivo de flag DQ criado: {flag_file_path}")
        except Exception as e: print(f"ERRO ao criar arquivo de flag DQ: {e}")

        allow_partial_success = global_config.get('error_handling', {}).get('allow_partial_success', False)
        if overall_status == "ERROR" and (not allow_partial_success or abort_requested):
            error_msg = f"Falha DQ na camada {target_layer} (allow_partial_success={allow_partial_success}, abort_requested={abort_requested})"
            # print(f"ERRO: {error_msg}. Abortando pipeline.")
            raise DataQualityError(error_msg, results_summary)
        return overall_status

# --- Classe de Erro Personalizada para DQ ---
class DataQualityError(Exception):
    def __init__(self, message, results=None):
        super().__init__(message)
        self.results = results if results is not None else []

# --- Classe Base Abstrata para Jobs ---
class BaseJob(ABC):
    """Classe base para jobs MDDA modulares."""
    def __init__(self, job_name="BaseJob"):
        self.job_name = job_name
        self.spark = None
        self.config_loader = ConfigLoader()
        self.catalog_manager = LocalCatalogManager()
        self.logger = None
        self.args = None
        self.pipeline_def = None
        self.orchestration_config = None
        self.execution_id = None # Será definido em execute()

    def _init_spark(self):
        if not self.spark:
            # print("INFO: Inicializando Spark Session...")
            try:
                app_name = f"{self.pipeline_id}_{self.__class__.__name__}" if hasattr(self, 'pipeline_id') and self.pipeline_id else self.job_name
                self.spark = SparkSession.builder \
                    .appName(app_name) \
                    .master("local[*]") \
                    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                    .config("spark.sql.shuffle.partitions", "1") \
                    .getOrCreate()
                # print("INFO: Spark Session inicializada.")
            except Exception as e:
                print(f"ERRO CRÍTICO ao inicializar Spark: {e}")
                raise

    def _stop_spark(self):
        if self.spark:
            # print("INFO: Parando Spark Session...")
            self.spark.stop(); self.spark = None
            # print("INFO: Spark Session parada.")

    def _setup_logger(self):
        if self.orchestration_config and self.pipeline_def and self.execution_id and not self.logger:
             log_base_path = "./data/logs/pipeline"
             try:
                 log_base_path_config = self.orchestration_config.get('alerting', {}) \
                                     .get('event_routing', {}) \
                                     .get('success', ["./data/logs/pipeline"])[0]
                 if log_base_path_config == "local_audit_log": log_base_path_config = "./data/logs/pipeline"
                 if isinstance(log_base_path_config, str) and log_base_path_config.strip():
                     log_base_path = log_base_path_config
             except Exception as e:
                 print(f"AVISO: Não foi possível obter log_destination_path do config, usando default '{log_base_path}'. Erro: {e}")

             self.logger = Logger(
                 pipeline_def=self.pipeline_def, orchestration_config=self.orchestration_config,
                 execution_id=self.execution_id, log_destination_path=log_base_path
             )

    def log(self, step_or_job_name, status, details):
        logger_name = self.job_name
        if hasattr(self, 'pipeline_id') and self.pipeline_id:
            logger_name = self.pipeline_id
        elif self.pipeline_def:
             logger_name = self.pipeline_def['metadata']['pipeline_id']

        if self.logger:
            self.logger.log_step(step_or_job_name, status, details)
        else:
             print(f"LOG_FALLBACK [{datetime.now().isoformat()}] {logger_name}/{self.execution_id}/{step_or_job_name} - Status: {status} - Details: {details}")

    def _parse_and_load_args(self):
        parser = argparse.ArgumentParser(description=f'{self.job_name} - MDDA Local Execution')
        parser.add_argument('--pipeline_def', required=True, help='Local path to 01-pipeline-definition-*.json')
        parser.add_argument('--orchestration_config', required=True, help='Local path to 05-orchestration-config-*.json')
        parser.add_argument('--execution_id', default=f"local_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}", help='Unique ID for the pipeline run')
        self.add_args(parser) # Permite que subclasses adicionem seus próprios argumentos
        self.args = parser.parse_args()
        # print(f"INFO: Argumentos recebidos para {self.__class__.__name__}: {vars(self.args)}")

        # Carrega configurações base e define atributos essenciais
        self.pipeline_def = self.config_loader.load_json_local(self.args.pipeline_def)
        self.orchestration_config = self.config_loader.load_json_local(self.args.orchestration_config)
        self.execution_id = self.args.execution_id
        self.pipeline_id = self.pipeline_def['metadata']['pipeline_id']
        # Atualiza o nome do job para ser mais específico
        self.job_name = f"{self.pipeline_id}_{self.__class__.__name__}"
        self._setup_logger() # Configura o logger agora que temos as configs e execution_id

    @abstractmethod
    def add_args(self, parser):
        """Método para subclasses adicionarem argumentos específicos ao parser."""
        pass

    @abstractmethod
    def run(self):
        """Método abstrato onde a lógica principal do job é implementada."""
        pass

    def execute(self):
        """Orquestra a execução completa do job, incluindo setup, run, e teardown."""
        start_time = datetime.now()
        try:
            self._parse_and_load_args() # Carrega args e configs, configura logger
        except Exception as parse_err:
             # Se o parse falhar, não temos como logar com o logger do framework
             print(f"ERRO CRÍTICO [{datetime.now().isoformat()}] ao parsear argumentos ou carregar configs para {self.job_name}: {parse_err}")
             sys.exit(1)

        # Log inicial após configs e logger estarem prontos
        print(f"\n==================================================")
        print(f" Iniciando Job: {self.job_name}")
        print(f" Execution ID: {self.execution_id}")
        print(f" Start Time: {start_time.isoformat()}")
        print(f"==================================================")
        self.log(self.job_name, "STARTING", {"args": vars(self.args)})

        try:
            self._init_spark() # Inicializa Spark (se a subclasse precisar)
            self.run() # Executa a lógica específica do job
            end_time = datetime.now(); duration = end_time - start_time
            self.log(self.job_name, "SUCCEEDED", {"duration": str(duration)})
            print(f"\n==================================================")
            print(f" Job {self.job_name} concluído com SUCESSO")
            print(f" End Time: {end_time.isoformat()} | Duration: {duration}")
            print(f"==================================================")
        except Exception as e:
            end_time = datetime.now(); duration = end_time - start_time
            error_details = {"error_type": type(e).__name__, "error_message": str(e)}
            if isinstance(e, DataQualityError): error_details["dq_results"] = e.results
            self.log(self.job_name, "FAILED", error_details) # Tenta logar o erro
            print(f"\n==================================================")
            print(f" Job {self.job_name} FALHOU")
            print(f" End Time: {end_time.isoformat()} | Duration: {duration}")
            print(f" Erro: {type(e).__name__}: {e}")
            print(f"==================================================")
            sys.exit(1) # Termina com erro
        finally:
            self._stop_spark() # Garante que o Spark seja parado