from csv_converter import CSVConverter

# Exemplo de uso
input_path = 'faturas/input/Fatura_2024-11-05.csv'
output_path = 'faturas/output/Fatura_2024-11-05.csv'
config_path = 'config.json'

converter = CSVConverter(input_path, output_path, config_path)
converter.convert()
