import csv
import random
from datetime import datetime, timedelta
import uuid
import os
import argparse

def generate_sales_data(target_date_str, num_rows, num_customers, products, statuses, status_weights, prob_negative_quantity, prob_null_price, prob_zero_price):
    """Gera dados de vendas para uma data específica."""
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    customers = [f"cliente{i}@email.com" for i in range(num_customers)] # Gera a lista de clientes
    data_rows = []

    # Cabeçalho
    header = ["order_id", "product_id", "quantity", "unit_price_brl", "order_timestamp", "customer_email", "status"]
    data_rows.append(header)

    # Gera as linhas de dados
    for i in range(num_rows):
        order_id = f"ORD{target_date.strftime('%Y%m%d')}{1000 + i:04d}" # Inclui data no ID
        product_id = random.choice(products)
        customer_email = random.choice(customers)
        status = random.choices(statuses, weights=status_weights, k=1)[0]

        # Quantidade (com anomalia)
        if random.random() < prob_negative_quantity and status == 'completed':
            quantity = random.randint(-5, -1)
        else:
            quantity = random.randint(1, 20)

        # Preço (com anomalias)
        if random.random() < prob_null_price and status == 'completed':
            unit_price_brl = ""
        elif random.random() < prob_zero_price and status == 'completed':
             unit_price_brl = f"{0.00:.2f}"
        else:
            base_price = (ord(product_id[-1]) - ord('A') + 1) * 15.50
            unit_price_brl = f"{random.uniform(base_price * 0.9, base_price * 1.1):.2f}"

        # Timestamp
        time_offset_seconds = random.randint(0, 86399)
        order_timestamp_dt = target_date + timedelta(seconds=time_offset_seconds)
        order_timestamp = order_timestamp_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        data_rows.append([
            order_id, product_id, quantity, unit_price_brl,
            order_timestamp, customer_email, status
        ])
    return data_rows

def main():
    # --- Configuração dos Argumentos ---
    parser = argparse.ArgumentParser(description="Gerador de Dados de Vendas CSV por Dia")
    parser.add_argument("--start_date", required=True, help="Data de início (YYYY-MM-DD)")
    parser.add_argument("--end_date", required=True, help="Data de fim (YYYY-MM-DD)")
    parser.add_argument("--rows_per_day", type=int, default=1000, help="Número de linhas por arquivo diário")
    parser.add_argument("--num_customers", type=int, default=1000, help="Número de clientes fictícios a gerar")
    parser.add_argument("--output_base_dir", default="./data/landing/sales/daily", help="Diretório base para salvar os arquivos CSV (serão criadas subpastas year=/month=/day=)")

    args = parser.parse_args()

    # --- Parâmetros Fixos de Geração ---
    products = [f"PROD_{chr(ord('A') + i)}" for i in range(10)]
    statuses = ["completed", "pending", "shipped", "canceled"]
    status_weights = [0.85, 0.05, 0.05, 0.05]
    prob_negative_quantity = 0.01
    prob_null_price = 0.015
    prob_zero_price = 0.005

    # --- Loop de Geração por Dia ---
    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    current_dt = start_dt

    print(f"Gerando dados de {args.start_date} até {args.end_date}...")
    print(f"Número de clientes: {args.num_customers}")
    print(f"Linhas por dia: {args.rows_per_day}")
    print(f"Diretório base de saída: {args.output_base_dir}")

    while current_dt <= end_dt:
        target_date_str = current_dt.strftime("%Y-%m-%d")
        print(f"\nGerando dados para: {target_date_str}")

        # Gera os dados para o dia atual
        daily_data = generate_sales_data(
            target_date_str, args.rows_per_day, args.num_customers, products,
            statuses, status_weights, prob_negative_quantity, prob_null_price, prob_zero_price
        )

        # Define o caminho de saída com estrutura de partição
        year = current_dt.year
        month = f"{current_dt.month:02d}"
        day = f"{current_dt.day:02d}"
        output_dir = os.path.join(args.output_base_dir, f"{year}", f"{month}", f"{day}")
        output_filename = f"vendas_{current_dt.strftime('%Y%m%d')}.csv"
        output_filepath = os.path.join(output_dir, output_filename)

        # Cria o diretório se não existir
        os.makedirs(output_dir, exist_ok=True)

        # Escreve o arquivo CSV para o dia
        try:
            with open(output_filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(daily_data)
            print(f"  Arquivo '{output_filepath}' gerado com sucesso ({len(daily_data) - 1} linhas).")
        except IOError as e:
            print(f"  Erro ao escrever o arquivo CSV para {target_date_str}: {e}")

        # Avança para o próximo dia
        current_dt += timedelta(days=1)

    print("\nGeração de dados concluída.")

if __name__ == "__main__":
    main()
