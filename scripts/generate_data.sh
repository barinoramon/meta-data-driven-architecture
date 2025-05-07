#!/bin/bash

# ==================================================
# Script: generate_data.sh
# Descrição: Executa o script Python para gerar dados de teste CSV.
# Usage: ./generate_data.sh [start_date] [end_date]
# Exemplo: ./generate_data.sh 2025-05-05 2025-05-07
# ==================================================

# --- Configurações Padrão ---
DEFAULT_START_DATE="2024-01-01"
DEFAULT_END_DATE="2025-12-31"
ROWS_PER_DAY=1000
NUM_CUSTOMERS=1000
OUTPUT_BASE_DIR="./data/landing/sales/daily"
PYTHON_SCRIPT_NAME="./scripts/generate_daily_test_data.py"

# --- Verifica Argumentos ---
START_DATE="${1:-$DEFAULT_START_DATE}"
END_DATE="${2:-$DEFAULT_END_DATE}"

# --- Valida datas no formato YYYY-MM-DD ---
validate_date() {
    if ! date -d "$1" &>/dev/null; then
        echo "Erro: Data inválida '$1'. Use o formato YYYY-MM-DD."
        exit 1
    fi
}

validate_date "$START_DATE"
validate_date "$END_DATE"

# --- Valida se o script Python existe ---
if [ ! -f "$PYTHON_SCRIPT_NAME" ]; then
    echo "Erro: Script Python '$PYTHON_SCRIPT_NAME' não encontrado."
    exit 1
fi

# --- Define o comando Python (Windows/Linux compatível) ---
if command -v py &>/dev/null; then
    PYTHON_CMD="py"
elif command -v python3 &>/dev/null; then
    PYTHON_CMD="python3"
else
    echo "Erro: Python não encontrado. Instale Python e adicione ao PATH."
    exit 1
fi

# --- Executa o Script Python ---
echo "Executando gerador de dados Python..."
echo "Período: $START_DATE a $END_DATE"
echo "Linhas/Dia: $ROWS_PER_DAY"
echo "Clientes: $NUM_CUSTOMERS"
echo "Saída: $OUTPUT_BASE_DIR"

$PYTHON_CMD "$PYTHON_SCRIPT_NAME" \
    --start_date "$START_DATE" \
    --end_date "$END_DATE" \
    --rows_per_day "$ROWS_PER_DAY" \
    --num_customers "$NUM_CUSTOMERS" \
    --output_base_dir "$OUTPUT_BASE_DIR"

# --- Verifica o código de saída ---
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "Erro: Script Python falhou com código $EXIT_CODE."
    exit $EXIT_CODE
else
    echo "Geração de dados concluída com sucesso."
fi

exit 0