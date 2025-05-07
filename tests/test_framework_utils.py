# Arquivo: tests/test_framework_utils.py
import unittest
import json
import os
import tempfile
import sys

# Adiciona o diretório pai (raiz do projeto) ao sys.path
# para que o módulo framework possa ser encontrado.
# Isso assume que 'tests' está um nível abaixo da raiz do projeto.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from framework.mdda_framework import ConfigLoader # Certifique-se que mdda_framework.py está em framework/

class TestConfigLoader(unittest.TestCase):

    def setUp(self):
        """Cria um arquivo JSON temporário para alguns testes."""
        self.config_loader = ConfigLoader()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.valid_json_content = {"key": "value", "number": 123, "nested": {"bool": True}}
        self.valid_json_path = os.path.join(self.temp_dir.name, "valid_temp.json")
        with open(self.valid_json_path, 'w', encoding='utf-8') as f:
            json.dump(self.valid_json_content, f)

        # Conteúdo problemático (exemplo com caractere de controle - Bell character \a)
        # Nota: Este é um exemplo, o erro real pode ser mais sutil.
        self.invalid_char_json_content_str = '{\n  "schema\u0007": "http://example.com"\n}' # \u0007 é o Bell char
        self.invalid_char_json_path = os.path.join(self.temp_dir.name, "invalid_char_temp.json")
        with open(self.invalid_char_json_path, 'w', encoding='utf-8') as f:
            f.write(self.invalid_char_json_content_str)

        # Caminho para o arquivo de definição do pipeline problemático (ajuste se necessário)
        # Assume que está em ./configs/sales_daily_br_v1/ a partir da raiz do projeto
        self.pipeline_def_path = os.path.join(
            project_root,
            "configs",
            "sales_daily_br_v1",
            "01-pipeline-definition-sales_daily_br_v1.json"
        )


    def tearDown(self):
        """Limpa o diretório temporário."""
        self.temp_dir.cleanup()

    def test_load_valid_json_from_temp_file(self):
        """Testa o carregamento de um arquivo JSON válido criado temporariamente."""
        print(f"\nINFO: Testando carregamento de JSON válido de: {self.valid_json_path}")
        loaded_data = self.config_loader.load_json_local(self.valid_json_path)
        self.assertEqual(loaded_data, self.valid_json_content)
        print("SUCCESS: Carregamento de JSON válido do arquivo temporário.")

    def test_load_non_existent_file(self):
        """Testa o comportamento ao tentar carregar um arquivo inexistente."""
        non_existent_path = os.path.join(self.temp_dir.name, "non_existent.json")
        print(f"\nINFO: Testando carregamento de arquivo inexistente: {non_existent_path}")
        with self.assertRaises(FileNotFoundError):
            self.config_loader.load_json_local(non_existent_path)
        print("SUCCESS: FileNotFoundError levantado corretamente para arquivo inexistente.")

    def test_load_json_with_problematic_chars_in_file(self):
        """
        Testa o carregamento de um JSON que pode ter caracteres problemáticos.
        A função load_json_local no framework tenta remover comentários.
        Este teste verifica se ela ainda falha com um caractere de controle real.
        """
        print(f"\nINFO: Testando carregamento de JSON com caractere de controle de: {self.invalid_char_json_path}")
        with self.assertRaises(json.JSONDecodeError) as context:
            self.config_loader.load_json_local(self.invalid_char_json_path)
        self.assertIn("Invalid control character", str(context.exception))
        print(f"SUCCESS: JSONDecodeError levantado para caractere de controle: {context.exception}")


    def test_load_actual_pipeline_definition_file(self):
        """
        Tenta carregar o arquivo 01-pipeline-definition-sales_daily_br_v1.json real.
        Se este teste falhar com 'Invalid control character', o problema está no arquivo.
        """
        print(f"\nINFO: Tentando carregar o arquivo real: {self.pipeline_def_path}")
        if not os.path.exists(self.pipeline_def_path):
            print(f"AVISO: Arquivo de definição do pipeline não encontrado em {self.pipeline_def_path}. Pulando este teste.")
            self.skipTest(f"Arquivo não encontrado: {self.pipeline_def_path}")
            return

        try:
            data = self.config_loader.load_json_local(self.pipeline_def_path)
            self.assertIsNotNone(data) # Verifica se algo foi carregado
            self.assertIn("metadata", data) # Verifica uma chave esperada
            print("SUCCESS: Arquivo 01-pipeline-definition-sales_daily_br_v1.json carregado com sucesso no teste.")
        except json.JSONDecodeError as e:
            print(f"ERRO NO TESTE: Falha ao decodificar {self.pipeline_def_path}: {e}")
            print(f"  Linha: {e.lineno}, Coluna: {e.colno}, Mensagem: {e.msg}")
            # Força a falha do teste se houver um JSONDecodeError
            self.fail(f"Falha ao decodificar {self.pipeline_def_path}: {e}")
        except Exception as e:
            print(f"ERRO NO TESTE: Erro inesperado ao carregar {self.pipeline_def_path}: {e}")
            self.fail(f"Erro inesperado ao carregar {self.pipeline_def_path}: {e}")


if __name__ == '__main__':
    unittest.main()
