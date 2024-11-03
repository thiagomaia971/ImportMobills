import pandas as pd
import csv
import json
from dateutil.relativedelta import relativedelta

class CSVConverter:
    def __init__(self, input_path, output_path, config_path):
        self.input_path = input_path
        self.output_path = output_path
        self.config_path = config_path
        self.data = None
        self.config = self.load_config()
        self.value_format = "int"  # Definindo o formato de valor como constante

    def load_config(self):
        """Carrega o arquivo de configuração JSON."""
        with open(self.config_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    
    def load_csv(self):
        """Carrega o arquivo CSV original."""
        self.data = pd.read_csv(self.input_path, sep=';')
    
    def process_columns(self):
        """Renomeia, reorganiza e filtra as colunas do CSV, define o valor da coluna 'Conta' e remove registros indesejados."""
        # Renomeia as colunas
        self.data = self.data.rename(columns={
            'Data de Compra': 'Data',
            'Descrição': 'Descrição',
            'Valor (em R$)': 'Valor',
            'Categoria': 'Categoria'
        })
        
        # Define o valor fixo para a coluna "Conta"
        self.data['Conta'] = self.config["fixed_account"]
        
        # Remove registros que contenham as descrições especificadas
        for desc in self.config["remove_descriptions"]:
            self.data = self.data[~self.data['Descrição'].str.contains(desc, case=False, na=False)]
        
        # Ajusta data e descrição de acordo com a parcela
        for index, row in self.data.iterrows():
            if pd.notna(row['Parcela']) and '/' in row['Parcela']:
                try:
                    parcela_atual, total_parcelas = map(int, row['Parcela'].split('/'))
                    
                    # Ajusta a data subtraindo o número de meses de acordo com a parcela
                    data_original = pd.to_datetime(row['Data'], dayfirst=True)
                    data_ajustada = data_original - relativedelta(months=(total_parcelas - parcela_atual + 1))
                    self.data.at[index, 'Data'] = data_ajustada.strftime('%d/%m/%Y')
                    
                    # Atualiza a descrição com a informação da parcela
                    descricao_ajustada = f"{row['Descrição']} ({row['Parcela']})"
                    self.data.at[index, 'Descrição'] = descricao_ajustada

                except ValueError:
                    print(f"Erro ao processar parcela para linha {index}: formato inválido.")

        # Formata a coluna "Valor"
        self.data['Valor'] = self.data['Valor'].apply(self.format_value)
        
        # Mapeia as categorias
        self.map_categories()

        # Seleciona e reorganiza as colunas no novo formato
        self.data = self.data[['Data', 'Descrição', 'Valor', 'Conta', 'Categoria']]
    
    def format_value(self, x):
        """Formata o valor conforme a configuração."""
        if self.value_format == "int" and x == int(x):
            return f"{int(x)}"
        return f"{x}"

    def map_categories(self):
        """Aplica o mapeamento de categorias de acordo com palavras-chave."""
        category_mapping = self.config["category_mappings"]

        # Mapeia as categorias que são listas de palavras-chave
        for new_category, conditions in category_mapping.items():
            for condition in conditions:
                # Verifica se a condição contém tanto "Descrição" quanto "Categoria"
                if "Descrição" in condition and condition["Descrição"] is not None and \
                   "Categoria" in condition and condition["Categoria"] is not None:
                    
                    # Quando ambas as condições estão presentes, usa AND (&)
                    mask = (self.data['Descrição'].str.contains(condition["Descrição"], case=False, na=False)) & \
                        (self.data['Categoria'] == condition["Categoria"])
                
                elif "Descrição" in condition and condition["Descrição"] is not None:
                    # Apenas "Descrição" presente
                    mask = self.data['Descrição'].str.contains(condition["Descrição"], case=False, na=False)
                
                elif "Categoria" in condition and condition["Categoria"] is not None:
                    # Apenas "Categoria" presente
                    mask = self.data['Categoria'] == condition["Categoria"]

                else:
                    # Se não houver condição válida, pula para a próxima
                    continue

                # Aplicando o mapeamento
                if mask.any():  # Verifica se há correspondências
                    print(f"Mapeando '{condition.get('Descrição', 'N/A')} - {condition.get('Categoria', 'N/A')}' para '{new_category}'")
                    self.data.loc[mask, 'Categoria'] = new_category



    def save_csv(self):
        """Salva o CSV com as colunas filtradas e reorganizadas no caminho de saída, com todos os valores entre aspas."""
        self.data.to_csv(
            self.output_path, 
            index=False, 
            sep=';', 
            quotechar='"', 
            quoting=csv.QUOTE_ALL
        )

    def convert(self):
        """Executa o processo completo de conversão."""
        self.load_csv()
        self.process_columns()
        self.save_csv()
