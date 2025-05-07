import json
import os

def generate_pipeline_definition(output_path):
    """
    Generates the content for 01-pipeline-definition-sales_daily_br_v1.json
    and saves it to the specified output_path.
    """
    pipeline_def_content = {
        "$schema": "https://json-schema.org/draft-07/schema#",
        "metadata": {
            "pipeline_id": "sales_daily_br_v1",
            "version": "1.0.0",
            "domain": "sales",
            "data_contract_id": "sales-transactions-daily-v1"
        },
        "source_config": {
            "bronze": {
                "location": "./data/landing/sales/daily/{yyyy}/{MM}/{dd}/",
                "format": "csv",
                "partition_template": "year={yyyy}/month={MM}/day={dd}",
                "freshness_rules": {
                    "expected_interval_hours": 24,
                    "alert_on_violation": True
                }
            }
        },
        "processing": {
            "transformations": [
                {
                    "template_id": "filter-status-completed-v1",
                    "parameters": {
                        "status_column": "status",
                        "desired_status": "completed"
                    },
                    "target_layer_step": "silver"
                },
                {
                    "template_id": "currency-conversion-brl-usd-v1",
                    "parameters": {
                        "source_column": "unit_price_brl",
                        "target_column": "unit_price_usd",
                        "conversion_rate": 5.0
                    },
                    "target_layer_step": "silver"
                },
                {
                    "template_id": "calculate-total-value-usd-v1",
                    "parameters": {
                        "quantity_column": "quantity",
                        "unit_price_column": "unit_price_usd",
                        "target_column": "total_value_usd"
                    },
                    "target_layer_step": "silver"
                },
                {
                    "template_id": "aggregate-daily-sales-by-product-v1",
                    "parameters": {
                        "group_by_columns": ["product_id", "order_date"],
                        "aggregation_specs": [
                            {"source_column": "quantity", "function": "sum", "target_column": "total_quantity"},
                            {"source_column": "total_value_usd", "function": "sum", "target_column": "total_aggregated_value_usd"}
                        ],
                        "timestamp_column_for_date": "order_timestamp"
                    },
                    "target_layer_step": "gold"
                }
            ],
            "processing_mode": "batch"
        },
        "destinations": [
            {
                "type": "data_lake",
                "target": "./data/silver/sales/sales_silver_salesdailybr/",
                "permissions": None
            },
            {
                "type": "data_lake",
                "target": "./data/gold/sales/sales_gold_salesdailybr/",
                "permissions": None
            }
        ],
        "governance": {
            "data_retention": None,
            "sla_tracking": None
        },
        "error_handling": {
            "retry_policy": {
                "max_attempts": 3,
                "backoff_seconds": 300
            },
            "quarantine_config": {
                "enabled": True,
                "path": "./data/quarantine/sales_daily_br_v1/"
            }
        }
    }

    # Garante que o diretório de configuração exista
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(pipeline_def_content, f, indent=2)
        print(f"Arquivo JSON '{output_path}' gerado/sobrescrito com sucesso e codificação UTF-8.")
    except IOError as e:
        print(f"Erro ao escrever o arquivo JSON em '{output_path}': {e}")
        raise

if __name__ == "__main__":
    # Define o caminho onde o arquivo JSON deve ser salvo
    # Ajuste este caminho se a sua estrutura de pastas for diferente
    project_root = os.path.dirname(os.path.abspath(__file__)) # Assume que o script está na raiz
    file_path = os.path.join(
        project_root,
        "configs",
        "sales_daily_br_v1",
        "01-pipeline-definition-sales_daily_br_v1.json"
    )
    generate_pipeline_definition(file_path)

