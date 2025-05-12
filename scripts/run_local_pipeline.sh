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

cd "$PROJECT_ROOT" || { echo "ERRO CRÍTICO: Não foi possível mudar para o diretório raiz do projeto: $PROJECT_ROOT"; exit 1; }
echo "INFO: Diretório de trabalho atual: $PWD"

# --- Tenta ativar o ambiente virtual Python (venv) ---
VENV_PATH="./venv"
PYTHON_EXEC=""

# echo "DEBUG: PATH antes da ativação do venv: $PATH" # Descomente para depurar PATH
if [ -d "$VENV_PATH" ]; then
    echo "INFO: Tentando ativar ambiente virtual em $VENV_PATH..."
    if [ -f "$VENV_PATH/Scripts/activate" ]; then # Windows/Git Bash
        echo "INFO: Ativando venv (Windows/Git Bash style)..."
        source "$VENV_PATH/Scripts/activate"
        if command -v python &> /dev/null; then
            PYTHON_EXEC="python"
        elif [ -f "$VENV_PATH/Scripts/python.exe" ]; then
            PYTHON_EXEC="$VENV_PATH/Scripts/python.exe"
        elif [ -f "$VENV_PATH/Scripts/python" ]; then
            PYTHON_EXEC="$VENV_PATH/Scripts/python"
        fi
    elif [ -f "$VENV_PATH/bin/activate" ]; then # Linux/WSL
        echo "INFO: Ativando venv (Linux/WSL style)..."
        source "$VENV_PATH/bin/activate"
        if command -v python3 &> /dev/null; then PYTHON_EXEC="python3";
        elif command -v python &> /dev/null; then PYTHON_EXEC="python"; fi
    else
        echo "AVISO: Script de ativação do venv não encontrado."
    fi
else
    echo "AVISO: Diretório venv '$VENV_PATH' não encontrado."
fi

if [ -z "$PYTHON_EXEC" ]; then
    echo "AVISO: venv não ativado ou Python não encontrado no venv. Tentando 'python'/'python3' globais."
    if command -v python &> /dev/null; then
        PYTHON_EXEC="python"
    elif command -v python3 &> /dev/null; then
        PYTHON_EXEC="python3"
    fi
fi
# echo "DEBUG: PATH após tentativa de ativação do venv: $PATH" # Descomente para depurar PATH

if ! command -v $PYTHON_EXEC &> /dev/null || [ -z "$PYTHON_EXEC" ]; then
    echo "ERRO CRÍTICO: Comando Python ('$PYTHON_EXEC' ou global) não encontrado. Verifique sua instalação Python/venv e o PATH."
    exit 1
fi
echo "INFO: Usando interpretador Python: $($PYTHON_EXEC --version 2>&1)"

# --- Configurações ---
PROCESSING_DATE="${1:-2025-05-05}"
PIPELINE_NAME="sales_daily_br_v1"

CONFIG_BASE_DIR="./configs"
JOBS_DIR="./jobs"
FRAMEWORK_DIR="./framework"

CONFIG_DIR="$CONFIG_BASE_DIR/$PIPELINE_NAME"
PIPELINE_DEF_PATH="$CONFIG_DIR/01-pipeline-definition-$PIPELINE_NAME.json"
DATA_CONTRACT_PATH="$CONFIG_DIR/02-data-contract-sales_transactions_daily-v1.json"
DQ_RULES_PATH="$CONFIG_DIR/04-data-quality-rules-sales.json"
ORCH_CONFIG_PATH="$CONFIG_DIR/05-orchestration-config-sales_daily.json"
GOV_MANIFEST_PATH="$CONFIG_DIR/06-governance-manifest-sales.json"
TEMPLATE_DIR="$CONFIG_DIR/templates/"
DQ_OUTPUT_DIR="./data/dq_results"
EXECUTION_ID="local_run_$(date +%Y%m%d_%H%M%S)_${RANDOM}"

# --- Validações Iniciais ---
echo "INFO: Validando arquivos e diretórios a partir de $PWD..."
# echo "DEBUG: Conteúdo do diretório atual ($PWD):"; ls -la # Descomente para depuração

if [ ! -d "$CONFIG_DIR" ]; then echo "ERRO: Diretório de configuração do pipeline não encontrado: $PWD/$CONFIG_DIR"; exit 1; fi
if [ ! -d "$JOBS_DIR" ]; then echo "ERRO: Diretório de jobs não encontrado: $PWD/$JOBS_DIR"; exit 1; fi
if [ ! -d "$FRAMEWORK_DIR" ]; then echo "ERRO: Diretório do framework não encontrado: $PWD/$FRAMEWORK_DIR"; exit 1; fi

JSON_FILES=("$PIPELINE_DEF_PATH" "$ORCH_CONFIG_PATH" "$DATA_CONTRACT_PATH" "$DQ_RULES_PATH" "$GOV_MANIFEST_PATH")
for json_file in "${JSON_FILES[@]}"; do
    if [ ! -f "$json_file" ]; then
        echo "ERRO: Arquivo JSON de configuração principal não encontrado: $PWD/$json_file"
        exit 1
    fi
done
echo "INFO: Validação inicial de diretórios e arquivos OK."

export PYTHONPATH="${PYTHONPATH}:$PWD/$FRAMEWORK_DIR"
echo "INFO: PYTHONPATH ajustado para incluir $PWD/$FRAMEWORK_DIR"

# --- Função para executar e verificar um job ---
run_job() {
    STEP_NAME=$1
    SCRIPT_BASENAME=$2
    shift 2 
    EXTRA_ARGS=("$@") 

    SCRIPT_PATH="$JOBS_DIR/$SCRIPT_BASENAME"

    echo "--------------------------------------------------"
    echo "Executando Step: $STEP_NAME"
    echo "Script: $SCRIPT_PATH"
    
    PYTHON_ARGS=(
        "$SCRIPT_PATH"
        --pipeline_def "$PIPELINE_DEF_PATH"
        --orchestration_config "$ORCH_CONFIG_PATH"
        --execution_id "$EXECUTION_ID"
    )
    PYTHON_ARGS+=("${EXTRA_ARGS[@]}")

    # Imprime o comando como um array para ver a tokenização
    echo "Argumentos para Python (tokenizados):"
    printf "  '%s'\n" "${PYTHON_ARGS[@]}"
    echo "--------------------------------------------------"

    "$PYTHON_EXEC" "${PYTHON_ARGS[@]}"

    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "ERRO: Step '$STEP_NAME' falhou com código de saída $EXIT_CODE."
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        "$PYTHON_EXEC" "$JOBS_DIR/4_logs_job.py" \
            --pipeline_def "$PIPELINE_DEF_PATH" \
            --orchestration_config "$ORCH_CONFIG_PATH" \
            --execution_id "$EXECUTION_ID" \
            --step_name PipelineEnd \
            --status FAILED \
            --log_details "{\"failed_step\": \"$STEP_NAME\", \"exit_code\": $EXIT_CODE}" > /dev/null 2>&1
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
run_job "CreateTable" "1_create_table_job.py" --data_contract "$DATA_CONTRACT_PATH"
run_job "Governance" "2_governance_job.py" --governance_manifest "$GOV_MANIFEST_PATH"
run_job "IngestBronze" "run_ingest_bronze.py" --processing_date "$PROCESSING_DATE"

BRONZE_YEAR=$(date -d "$PROCESSING_DATE" +%Y)
BRONZE_MONTH=$(date -d "$PROCESSING_DATE" +%m)
BRONZE_DAY=$(date -d "$PROCESSING_DATE" +%d)
DOMAIN_LOWER=$(echo "$PIPELINE_NAME" | cut -d'_' -f1)
PIPELINE_ID_SHORT=$(echo "$PIPELINE_NAME" | sed "s/${DOMAIN_LOWER}_//;s/_v[0-9].*//")
BRONZE_TABLE_NAME="${DOMAIN_LOWER}_bronze_${PIPELINE_ID_SHORT}v1"
BRONZE_DATA_PATH_FOR_DQ="./data/bronze/${DOMAIN_LOWER}/${BRONZE_TABLE_NAME}/year=${BRONZE_YEAR}/month=${BRONZE_MONTH}/day=${BRONZE_DAY}/"

# Modificação aqui: Usa aspas simples em volta da string JSON para --execution_metrics_json
METRICS_JSON_BRONZE='{"last_data_processed_date": "'"$PROCESSING_DATE"'"}'
run_job "UpdateCatalogBronze" "5_glue_catalog_job.py" --data_contract "$DATA_CONTRACT_PATH" --governance_manifest "$GOV_MANIFEST_PATH" --target_layer bronze --execution_metrics_json "$METRICS_JSON_BRONZE"

run_job "DQChecksSilver" "3_data_quality_job.py" --dq_rules "$DQ_RULES_PATH" --input_data_path "$BRONZE_DATA_PATH_FOR_DQ" --input_format parquet --target_layer silver --processing_date "$PROCESSING_DATE" --dq_output_path "$DQ_OUTPUT_DIR"

run_job "TransformSilver" "run_transform_silver.py" --template_dir "$TEMPLATE_DIR" --processing_date "$PROCESSING_DATE" --dq_output_path "$DQ_OUTPUT_DIR"
METRICS_JSON_SILVER='{"last_data_processed_date": "'"$PROCESSING_DATE"'"}'
run_job "UpdateCatalogSilver" "5_glue_catalog_job.py" --data_contract "$DATA_CONTRACT_PATH" --governance_manifest "$GOV_MANIFEST_PATH" --target_layer silver --execution_metrics_json "$METRICS_JSON_SILVER"

run_job "TransformGold" "run_transform_gold.py" --template_dir "$TEMPLATE_DIR"
METRICS_JSON_GOLD='{"last_data_processed_date": "'"$PROCESSING_DATE"'"}'
run_job "UpdateCatalogGold" "5_glue_catalog_job.py" --data_contract "$DATA_CONTRACT_PATH" --governance_manifest "$GOV_MANIFEST_PATH" --target_layer gold --execution_metrics_json "$METRICS_JSON_GOLD"

run_job "PipelineEnd" "4_logs_job.py" --step_name PipelineEnd --status SUCCEEDED --log_details "{}"

echo "=================================================="
echo " Pipeline $PIPELINE_NAME concluído com SUCESSO!"
echo " Execution ID: $EXECUTION_ID"
echo "=================================================="

exit 0
