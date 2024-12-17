import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import json
import re


# URLs que vou usar
urls = [
    "https://en.wikipedia.org/wiki/List_of_national_capitals_by_population",
    "https://en.wikipedia.org/wiki/List_of_national_capitals_by_area",
    "https://en.wikipedia.org/wiki/List_of_national_capitals_by_latitude"
]

# Realiza requisições HTTP e retorna o HTML da página
def fetch_html(url):
    requisicao = requests.get(url)
    if requisicao.status_code == 200:
        return requisicao.text
    else:
        print(f"Erro ao acessar {url}: {requisicao.status_code}")
        return None

# Extrai informações desejadas do HTML
def parse_html(html, url):
    site = BeautifulSoup(html, "html.parser")
    data = {
        "title": site.find("title").text if site.find("title") else "N/A",
        "header": site.find("h1").text if site.find("h1") else "N/A",
        "url": url
    }

    tabela = site.find("table", {"class": "wikitable"})
    if tabela:
        cabecalhos = [th.text.strip() for th in tabela.find_all("th")]

        linhas = []
        for linha in tabela.find_all("tr"):
            celulas = [td.text.strip() for td in linha.find_all("td")]

            # Ajusta a quantidade de células para coincidir com os cabeçalhos
            if celulas:
                while len(celulas) < len(cabecalhos):
                    celulas.append(None)  # Preenche com None se faltarem colunas
                while len(celulas) > len(cabecalhos):
                    celulas = celulas[:len(cabecalhos)]  # Trunca se houver colunas em excesso
                linhas.append(celulas)

        # Conversão dos dados em DataFrame do Pandas
        if cabecalhos and linhas:
            data["table"] = pd.DataFrame(linhas, columns=cabecalhos).to_dict(orient="records")
        else:
            data["table"] = "Tabela sem dados estruturados"
    else:
        data["table"] = "Nenhuma tabela encontrada"

    return data

# Realiza o scraping de uma lista de URLs
def scrape_data(sites):
    dataset = []
    for url in sites:
        print(f"Processando: {url}")
        html = fetch_html(url)
        if html:
            data = parse_html(html, url)
            dataset.append(data)
    return dataset


# Carregar o dataset original
def load_dataset(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        return json.load(file)

# Função para limpar nomes (remove asteriscos e espaços extras)
def clean_name(name):
    if name:
        return re.sub(r'[\*\u202f\u200b]', '', name).strip()  # Remove *, espaços Unicode e extras
    return None

# Cruzar os dados e consolidar em um único objeto
def consolidate_data(dataset):
    country_data = {}

    for entry in dataset:
        url = entry["url"]
        table = entry["table"]
        
        for row in table:
            # Identificar nomes de atributos dinâmicos
            country_keys = ["Country / dependency", "Country/Territory", "Country"]
            capital_keys = ["Capital", "City"]
            latitude_key = next((key for key in row.keys() if "Latitude" in key), None)
            
            # Extração dos atributos
            country = next((row[key] for key in country_keys if key in row), None)
            capital = next((row[key] for key in capital_keys if key in row), None)
            population = row.get("Population")
            area = row.get("Area")
            latitude = row.get(latitude_key) if latitude_key else None

            if not country or not capital:
                continue  # Ignorar entradas inválidas
            
            # Limpar o nome do país e normalizar a chave
            country = clean_name(country)
            capital = clean_name(capital)

            if country not in country_data:
                country_data[country] = {
                    "Country": country,
                    "Capital": capital,
                    "Population": None,
                    "Area": None,
                    "Latitude": None,
                    "URLs": set()
                }

            # Atualizar os dados consolidados
            if population:
                country_data[country]["Population"] = population
            if area:
                country_data[country]["Area"] = area
            if latitude:
                country_data[country]["Latitude"] = latitude
            country_data[country]["URLs"].add(url)

    # Converter o conjunto de URLs em lista
    for country in country_data:
        country_data[country]["URLs"] = list(country_data[country]["URLs"])
    
    return list(country_data.values())

# Salvar o novo JSON consolidado
def save_to_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

# Função principal
def main():
    dataset = load_dataset('dataset.json')
    consolidated_data = consolidate_data(dataset)
    save_to_json(consolidated_data, 'consolidated_dataset.json')
    print("Consolidação concluída! Dados salvos em 'consolidated_dataset.json'.")

if __name__ == "__main__":
    main()
