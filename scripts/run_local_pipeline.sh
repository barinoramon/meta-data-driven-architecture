#!/bin/bash

# ==================================================
# Script: run_local_pipeline.sh
# Descrição: Orquestra a execução local do pipeline MDDA para um dia específico.
# Usage: ./scripts/run_local_pipeline.sh [processing_date]
# Exemplo: ./scripts/run_local_pipeline.sh 2025-05-05
# IMPORTANTE: Execute este script a partir da RAIZ do projeto.
# ==================================================

# --- Get Script Directory and Project Root ---
SCRIPT_DIR_RAW=$(dirname -- "${BASH_SOURCE[0]}")
SCRIPT_DIR=$(cd -- "$SCRIPT_DIR_RAW" &> /dev/null && pwd)
PROJECT_ROOT=$(cd -- "$SCRIPT_DIR/.." &> /dev/null && pwd)

echo "INFO: Raiz do Projeto detectada: $PROJECT_ROOT"
echo "INFO: Diretório do script: $SCRIPT_DIR"

# Muda para o diretório raiz do projeto para garantir que os caminhos relativos funcionem
cd "$PROJECT_ROOT" || { echo "ERRO CRÍTICO: Não foi possível mudar para o diretório raiz do projeto: $PROJECT_ROOT"; exit 1; }
echo "INFO: Diretório de trabalho atual: $PWD"

# --- Tenta ativar o ambiente virtual Python (venv) ---
# Ajuste o caminho para seu venv se for diferente
VENV_PATH="./venv" # Caminho padrão para venv na raiz do projeto
PYTHON_EXEC=""

echo "DEBUG: PATH antes da ativação do venv: $PATH"

if [ -d "$VENV_PATH" ]; then
    echo "INFO: Tentando ativar ambiente virtual em $VENV_PATH..."
    if [ -f "$VENV_PATH/Scripts/activate" ]; then # Para Git Bash / MSYS (Windows)
        echo "INFO: Ativando venv (Windows/Git Bash style)..."
        source "$VENV_PATH/Scripts/activate"
        # No Windows, o executável dentro do venv/Scripts é python.exe
        # Tentar caminhos explícitos se 'python' não funcionar diretamente após source
        if command -v python &> /dev/null; then
            PYTHON_EXEC="python"
        elif [ -f "$VENV_PATH/Scripts/python.exe" ]; then
            PYTHON_EXEC="$VENV_PATH/Scripts/python.exe"
            echo "INFO: Usando caminho explícito: $PYTHON_EXEC"
        elif [ -f "$VENV_PATH/Scripts/python" ]; then
            PYTHON_EXEC="$VENV_PATH/Scripts/python"
            echo "INFO: Usando caminho explícito: $PYTHON_EXEC"
        else
             echo "AVISO: 'python' não encontrado no PATH após source, e executáveis explícitos não encontrados em $VENV_PATH/Scripts."
        fi
    elif [ -f "$VENV_PATH/bin/activate" ]; then # Para WSL / Linux
        echo "INFO: Ativando venv (Linux/WSL style)..."
        source "$VENV_PATH/bin/activate"
        if command -v python3 &> /dev/null; then
            PYTHON_EXEC="python3"
        elif command -v python &> /dev/null; then
            PYTHON_EXEC="python"
        else
            echo "AVISO: Nem 'python3' nem 'python' encontrados no PATH após source em $VENV_PATH/bin/activate."
        fi
    else
        echo "AVISO: Script de ativação do venv não encontrado em $VENV_PATH/Scripts/activate ou $VENV_PATH/bin/activate."
    fi
else
    echo "AVISO: Diretório venv '$VENV_PATH' não encontrado."
fi

# Se PYTHON_EXEC ainda estiver vazio, tenta 'python' e 'python3' globais
if [ -z "$PYTHON_EXEC" ]; then
    echo "AVISO: venv não ativado ou Python não encontrado no venv. Tentando 'python'/'python3' globais."
    if command -v python &> /dev/null; then
        PYTHON_EXEC="python"
    elif command -v python3 &> /dev/null; then
        PYTHON_EXEC="python3"
    fi
fi

echo "DEBUG: PATH após tentativa de ativação do venv: $PATH"

# Verifica se o comando python agora funciona
if ! command -v $PYTHON_EXEC &> /dev/null || [ -z "$PYTHON_EXEC" ]; then
    echo "ERRO CRÍTICO: Comando Python ('$PYTHON_EXEC' ou global) não encontrado. Verifique sua instalação Python/venv e o PATH."
    exit 1
fi
echo "INFO: Usando interpretador Python: $($PYTHON_EXEC --version 2>&1)"


# --- Configurações (agora relativas à raiz do projeto, pois fizemos cd) ---
PROCESSING_DATE="${1:-2025-05-05}"
PIPELINE_NAME="sales_daily_br_v1"

CONFIG_BASE_DIR="./configs"
JOBS_DIR="./jobs"
FRAMEWORK_DIR="./framework"

CONFIG_DIR="$CONFIG_BASE_DIR/$PIPELINE_NAME"
PIPELINE_DEF="$CONFIG_DIR/01-pipeline-definition-$PIPELINE_NAME.json"
DATA_CONTRACT="$CONFIG_DIR/02-data-contract-sales_transactions_daily-v1.json"
DQ_RULES="$CONFIG_DIR/04-data-quality-rules-sales.json"
ORCH_CONFIG="$CONFIG_DIR/05-orchestration-config-sales_daily.json"
GOV_MANIFEST="$CONFIG_DIR/06-governance-manifest-sales.json"
TEMPLATE_DIR="$CONFIG_DIR/templates/"
DQ_OUTPUT_DIR="./data/dq_results"
EXECUTION_ID="local_run_$(date +%Y%m%d_%H%M%S)_${RANDOM}"

# --- Validações Iniciais ---
echo "INFO: Validando arquivos e diretórios a partir de $PWD..."
# Lista o conteúdo do diretório atual para depuração
echo "DEBUG: Conteúdo do diretório atual ($PWD):"
ls -la

if [ ! -d "$CONFIG_DIR" ]; then echo "ERRO: Diretório de configuração do pipeline não encontrado: $PWD/$CONFIG_DIR"; exit 1; fi
if [ ! -d "$JOBS_DIR" ]; then echo "ERRO: Diretório de jobs não encontrado: $PWD/$JOBS_DIR"; exit 1; fi
if [ ! -d "$FRAMEWORK_DIR" ]; then echo "ERRO: Diretório do framework não encontrado: $PWD/$FRAMEWORK_DIR"; exit 1; fi

# Validação de arquivos JSON principais
JSON_FILES=("$PIPELINE_DEF" "$ORCH_CONFIG" "$DATA_CONTRACT" "$DQ_RULES" "$GOV_MANIFEST")
for json_file in "${JSON_FILES[@]}"; do
    if [ ! -f "$json_file" ]; then
        echo "ERRO: Arquivo JSON de configuração principal não encontrado: $PWD/$json_file"
        exit 1
    fi
done
echo "INFO: Validação inicial de diretórios e arquivos OK."

# Adiciona o diretório do framework ao PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$PWD/$FRAMEWORK_DIR" # Usa $PWD para garantir caminho absoluto após cd
echo "INFO: PYTHONPATH ajustado para incluir $PWD/$FRAMEWORK_DIR"

# --- Função para executar e verificar um job ---
run_job() {
    STEP_NAME=$1
    SCRIPT_BASENAME=$2 # Apenas o nome base do script, ex: 4_logs_job.py
    shift 2
    EXTRA_ARGS="$@"
    SCRIPT_PATH="$JOBS_DIR/$SCRIPT_BASENAME" # Constrói o caminho completo

    echo "--------------------------------------------------"
    echo "Executando Step: $STEP_NAME"
    echo "Script: $SCRIPT_PATH"
    echo "Argumentos Extras: $EXTRA_ARGS"
    echo "Comando: $PYTHON_EXEC $SCRIPT_PATH --pipeline_def \"$PIPELINE_DEF\" --orchestration_config \"$ORCH_CONFIG\" --execution_id \"$EXECUTION_ID\" $EXTRA_ARGS"
    echo "--------------------------------------------------"

    "$PYTHON_EXEC" "$SCRIPT_PATH" \
        --pipeline_def "$PIPELINE_DEF" \
        --orchestration_config "$ORCH_CONFIG" \
        --execution_id "$EXECUTION_ID" \
        $EXTRA_ARGS

    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "ERRO: Step '$STEP_NAME' falhou com código de saída $EXIT_CODE."
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        "$PYTHON_EXEC" "$JOBS_DIR/4_logs_job.py" --pipeline_def "$PIPELINE_DEF" --orchestration_config "$ORCH_CONFIG" --execution_id "$EXECUTION_ID" --step_name PipelineEnd --status FAILED --log_details "{\"failed_step\": \"$STEP_NAME\", \"exit_code\": $EXIT_CODE}" > /dev/null 2>&1
        exit $EXIT_CODE
    else
        echo "Step '$STEP_NAME' concluído com sucesso."
    fi
}

# --- Orquestração da Execução ---
echo "=================================================="
echo " Iniciando Pipeline: $PIPELINE_NAME"
echo " Data de Processamento: $PROCESSING_DATE"
echo " Execution ID: $EXECUTION_ID"
echo "=================================================="

run_job "PipelineStart" "4_logs_job.py" --step_name PipelineStart --status STARTING --log_details "{}"
run_job "CreateTable" "1_create_table_job.py" --data_contract "$DATA_CONTRACT"
run_job "Governance" "2_governance_job.py" --governance_manifest "$GOV_MANIFEST"
run_job "IngestBronze" "run_ingest_bronze.py" --processing_date "$PROCESSING_DATE"

BRONZE_YEAR=$(date -d "$PROCESSING_DATE" +%Y)
BRONZE_MONTH=$(date -d "$PROCESSING_DATE" +%m)
BRONZE_DAY=$(date -d "$PROCESSING_DATE" +%d)
# Ajuste este caminho se o nome da tabela bronze for diferente no seu manifesto
# BRONZE_TABLE_OUTPUT_PATH="./data/bronze/$PIPELINE_NAME/year=$BRONZE_YEAR/month=$BRONZE_MONTH/day=$BRONZE_DAY/"
# O nome da tabela no manifesto é sales_bronze_dailybrv1, então o path é ./data/bronze/sales/sales_bronze_dailybrv1/...
# Vamos tentar construir o nome da tabela dinamicamente para o path de entrada do DQ
DOMAIN_LOWER=$(echo "$PIPELINE_NAME" | cut -d'_' -f1) # Assume 'sales'
PIPELINE_ID_SHORT=$(echo "$PIPELINE_NAME" | sed "s/${DOMAIN_LOWER}_//;s/_v[0-9].*//") # ex: dailybr
BRONZE_TABLE_NAME="${DOMAIN_LOWER}_bronze_${PIPELINE_ID_SHORT}v1" # ex: sales_bronze_dailybrv1
BRONZE_DATA_PATH_FOR_DQ="./data/bronze/${DOMAIN_LOWER}/${BRONZE_TABLE_NAME}/year=${BRONZE_YEAR}/month=${BRONZE_MONTH}/day=${BRONZE_DAY}/"

run_job "UpdateCatalogBronze" "5_glue_catalog_job.py" --data_contract "$DATA_CONTRACT" --governance_manifest "$GOV_MANIFEST" --target_layer bronze --execution_metrics_json "{\"last_data_processed_date\": \"$PROCESSING_DATE\"}"
run_job "DQChecksSilver" "3_data_quality_job.py" --dq_rules "$DQ_RULES" --input_data_path "$BRONZE_DATA_PATH_FOR_DQ" --input_format parquet --target_layer silver --processing_date "$PROCESSING_DATE" --dq_output_path "$DQ_OUTPUT_DIR"
run_job "TransformSilver" "run_transform_silver.py" --template_dir "$TEMPLATE_DIR" --processing_date "$PROCESSING_DATE" --dq_output_path "$DQ_OUTPUT_DIR"
run_job "UpdateCatalogSilver" "5_glue_catalog_job.py" --data_contract "$DATA_CONTRACT" --governance_manifest "$GOV_MANIFEST" --target_layer silver --execution_metrics_json "{\"last_data_processed_date\": \"$PROCESSING_DATE\"}"
run_job "TransformGold" "run_transform_gold.py" --template_dir "$TEMPLATE_DIR"
run_job "UpdateCatalogGold" "5_glue_catalog_job.py" --data_contract "$DATA_CONTRACT" --governance_manifest "$GOV_MANIFEST" --target_layer gold --execution_metrics_json "{\"last_data_processed_date\": \"$PROCESSING_DATE\"}"
run_job "PipelineEnd" "4_logs_job.py" --step_name PipelineEnd --status SUCCEEDED --log_details "{}"

echo "=================================================="
echo " Pipeline $PIPELINE_NAME concluído com SUCESSO!"
echo " Execution ID: $EXECUTION_ID"
echo "=================================================="

exit 0