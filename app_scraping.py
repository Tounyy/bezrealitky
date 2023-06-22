# Potřebný knihovny
import requests
from bs4 import BeautifulSoup as bs
import numpy as np
import datetime
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import os


# Funkce pro scrapování odkazy z pronájmu
def vsechny_odkazy_pronajmu(url, max_pages):
    # Prázdný seznam slouží k uložení odkazů z pronájmu
    all_links = []

    def process_page(page):
        # Vytvoření URL s aktuální stránkou
        current_url = f"{url}&page={page}"
        # Získání obsahu stránky
        response = requests.get(current_url)
        # Parsování HTML obsahu
        soup = bs(response.content, "html.parser")
        # Najití všech článků s nabídkami pronájmu
        article_elements = soup.find_all("article", {
                                         "class": "PropertyCard_propertyCard__qPQRK propertyCard PropertyCard_propertyCard--landscape__7grmL"})
        # Extrahování odkazů z článků
        links = [article_element.find("div", {"class": "PropertyCard_propertyCardImageHolder__Kn1CN mb-3 mb-md-0 me-md-5 propertyCardImageHolder"}).find(
            "a")["href"] for article_element in article_elements]
        # Vrácení hodnoty "links"
        return links

    # Paralelní scrapování odkazů na více stránkách
        # ThreadPoolExecutor = umožňuje současně spouštět úlohy v rámci více vláken, což zlepšuje výkon a urychluje zpracování úloh.
        # Executor = používána k odesílání úloh k provedení v rámci více vláken
    with ThreadPoolExecutor() as executor:
        # Seznam čísel stránek
        pages = range(1, max_pages + 1)
        # Procházení stránek a scrapování odkazů
        results = list(tqdm(executor.map(process_page, pages),
                       total=max_pages, desc="Scraping links"))

    # Sloučení odkazů z různých stránek do jednoho seznamu
    all_links = [link for sublist in results for link in sublist]
    return all_links


# Scrapování dat pro jednotlivé nabídky pronájmu
def scrape_data_pronajmu(url, index, offer_type):
    # Získání obsahu stránky
    response = requests.get(url)
    # Parsování HTML obsahu
    soup = bs(response.content, "html.parser")
    # Najití všech řádků s informacemi o nabídce
    rows = soup.find_all('tr')
    # Uložení hodnot do slovníku
    values = {}

    # Procházení jednotlivých řádků a extrakce informací
    for row in rows:
        header = row.find('th').text.strip()  # Název sloupce
        data = row.find('td')  # Hodnota v buňce
        if data:
            data = data.text.strip()
        else:
            data = ''
        if header in ['Číslo inzerátu', 'Dispozice', 'Plocha', 'Vybaveno', 'Stav']:
            values[header] = data

    # Získání informace o cenách
    cena_1 = soup.find("div", {"class": "mb-lg-9 mb-6"})
    cena_2 = ''
    if cena_1:
        cena_2_element = cena_1.find(
            "div", {"class": "justify-content-between align-items-baseline row"})
        if cena_2_element:
            cena_2 = cena_2_element.find("strong", {"class": "h4 fw-bold"})
            if cena_2:
                cena_2 = cena_2.text.strip()
    values['Cena'] = cena_2

    # Získání informace o adresy
    adresa = soup.find(
        "div", {"class": "col-xxl-8 col-xl-9 col-lg-10 col-md-9"})
    adresa_text = ''
    if adresa:
        adresa_span = adresa.find(
            "span", {"class": "d-block text-perex-lg text-grey-dark"})
        if adresa_span:
            adresa_text = adresa_span.text.strip()

    values['Adresa'] = adresa_text

    # Získání informace o plochách
    plocha = values.get('Plocha', '')
    if plocha:
        # replace slouží k nahrazení části řetězce jiným řetězcem
        plocha = plocha.replace(' m²', '').replace(',', '.')
        plocha = float(plocha)

    # Získání informace o ceně a její příprava
    cena = values.get('Cena', '')
    if cena:
        # Ponechání pouze číslic v ceně
        cena = ''.join(filter(str.isdigit, cena))
        # Převod ceny na desetinné číslo nebo None, pokud cena není platná
        cena = float(cena) if cena else None

    # Výpočet ceny za 1 metr čtvereční
    cena_1m2 = round(cena / plocha, 2) if cena and plocha else None

    url = "https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/"
    response = requests.get(url)
    soup = bs(response.content, "html.parser")

    euro_row_index = 6  # Index řádku, na kterém se nachází euro
    euro_row = soup.find(
        "table", {"class": "currency-table"}).find_all("tr")[euro_row_index]
    euro_value_index = 4  # Index buňky, ve které se nachází hodnota eura
    euro_value = euro_row.find_all("td")[euro_value_index].text
    currency_value_2 = float(euro_value.replace(",", "."))

    if '€' in values['Cena']:
        cena_1m2 = cena_1m2 * currency_value_2
        values['Cena za m²'] = f"{cena_1m2:,.2f} Kč"
    else:
        values['Cena za m²'] = " "

    if 'Kč' in values['Cena']:
        values['Cena za m²'] = f"{cena_1m2:,.2f} Kč"
    else:
        values['Cena za m²'] = " "

    # Získání informace jestli je pronájem nebo prodej
    typ_nemovitosti_elements = soup.find_all(
        "span", {"class": "PropertyCard_propertyCardLabel__lnHZu mb-2 text-caption text-grey-dark fw-medium text-uppercase text-truncate"})
    typ_nemovitosti = [typ.text.strip().lower()
                       for typ in typ_nemovitosti_elements]
    typ = offer_type if offer_type in typ_nemovitosti else offer_type.capitalize()
    values['Typ'] = typ

    # Vytvoření DataFrame s hodnotami nemovitosti
    df = pd.DataFrame([values])
    parametry_nemovitosti1 = soup.find(
        "section", {"class": "box Section_section___TusU section mb-5 mb-lg-10"})
    parametry_nemovitosti2 = ''
    if parametry_nemovitosti1:
        parametry_nemovitosti2 = parametry_nemovitosti1.find(
            "h2", {"class": "mb-3 mb-lg-8 text-subheadline"})
        if parametry_nemovitosti2:
            parametry_nemovitosti2 = parametry_nemovitosti2.text.strip()
    # Nastavení názvu DataFrame na hodnotu parametrů nemovitosti
    df.name = parametry_nemovitosti2
    df.index = [index]  # Nastavení indexu DataFrame na hodnotu indexu
    # Nahrazení chybějících hodnot prázdnými řetězci
    df = df.replace(np.nan, '')
    return df  # Vrací vytvořený DataFrame s hodnotami nemovitosti


def vsechny_odkazy_prodej(url, max_pages):
    all_links = []

    def process_page(page):
        current_url = f"{url}&page={page}"
        response = requests.get(current_url)
        soup = bs(response.content, "html.parser")
        article_elements = soup.find_all("article", {
                                         "class": "PropertyCard_propertyCard__qPQRK propertyCard PropertyCard_propertyCard--landscape__7grmL PropertyCard_propertyCard--disable-link-mask__E6BVo"})
        links = [article_element.find("div", {"class": "PropertyCard_propertyCardImageHolder__Kn1CN mb-3 mb-md-0 me-md-5 propertyCardImageHolder"}).find(
            "a")["href"] for article_element in article_elements if article_element.find("div", {"class": "PropertyCard_propertyCardImageHolder__Kn1CN mb-3 mb-md-0 me-md-5 propertyCardImageHolder"})]
        return links

    with ThreadPoolExecutor() as executor:
        pages = range(1, max_pages + 1)
        results = list(tqdm(executor.map(process_page, pages),
                       total=max_pages, desc="Scraping links"))

    all_links = [link for sublist in results for link in sublist]
    return all_links


def scrape_data_prodej(url, index):
    response = requests.get(url)
    soup = bs(response.content, "html.parser")
    rows = soup.find_all('tr')
    values = {}

    for row in rows:
        header = row.find('th').text.strip()
        data = row.find('td')
        if data:
            data = data.text.strip()
        else:
            data = ''
        if header in ['Číslo inzerátu', 'Dispozice', 'Plocha', 'Vybaveno', 'Stav']:
            values[header] = data

    cena_1 = soup.find("div", {"class": "box mt-6 d-md-none"})
    cena_2 = ''
    if cena_1:
        cena_2_element = cena_1.find(
            "div", {"class": "justify-content-between align-items-baseline mb-lg-9 mb-6 row"})
        if cena_2_element:
            cena_2 = cena_2_element.find("strong", {"class": "h4 fw-bold"})
            if cena_2:
                cena_2 = cena_2.text.strip()
    values['Cena'] = cena_2

    adresa = soup.find(
        "div", {"class": "col-xxl-8 col-xl-9 col-lg-10 col-md-9"})
    adresa_text = ''
    if adresa:
        adresa_span = adresa.find(
            "span", {"class": "d-block text-perex-lg text-grey-dark"})
        if adresa_span:
            adresa_text = adresa_span.text.strip()

    values['Adresa'] = adresa_text

    plocha = values.get('Plocha', '')
    if plocha:
        plocha = plocha.replace(' m²', '').replace(',', '.')
        plocha = float(plocha)

    cena = values.get('Cena', '')
    if cena:
        cena = ''.join(filter(str.isdigit, cena))
        cena = float(cena) if cena else None

    cena_1m2 = round(cena / plocha, 2) if cena and plocha else None

    url = "https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/"
    response = requests.get(url)
    soup = bs(response.content, "html.parser")

    euro_row_index = 6  # Index řádku, na kterém se nachází euro
    euro_row = soup.find(
        "table", {"class": "currency-table"}).find_all("tr")[euro_row_index]
    euro_value_index = 4  # Index buňky, ve které se nachází hodnota eura
    euro_value = euro_row.find_all("td")[euro_value_index].text
    currency_value_2 = float(euro_value.replace(",", "."))

    if '€' in values['Cena']:
        cena_1m2 = cena_1m2 * currency_value_2
        values['Cena za m²'] = f"{cena_1m2:,.2f} Kč"
    else:
        values['Cena za m²'] = " "

    if 'Kč' in values['Cena']:
        values['Cena za m²'] = f"{cena_1m2:,.2f} Kč"
    else:
        values['Cena za m²'] = " "

    typ_nemovitosti_elements = soup.find_all(
        "span", {"class": "PropertyCard_propertyCardLabel__lnHZu mb-2 text-caption text-grey-dark fw-medium text-uppercase text-truncate"})
    typ_nemovitosti = [typ.text.strip().lower()
                       for typ in typ_nemovitosti_elements]
    typ = "Prodej" if "prodej" in typ_nemovitosti else "Prodej"
    values['Typ'] = typ

    df = pd.DataFrame([values])
    parametry_nemovitosti1 = soup.find(
        "section", {"class": "box Section_section___TusU section mb-5 mb-lg-10"})
    parametry_nemovitosti2 = ''
    if parametry_nemovitosti1:
        parametry_nemovitosti2 = parametry_nemovitosti1.find(
            "h2", {"class": "mb-3 mb-lg-8 text-subheadline"})
        if parametry_nemovitosti2:
            parametry_nemovitosti2 = parametry_nemovitosti2.text.strip()

    df.name = parametry_nemovitosti2
    df.index = [index]
    df = df.replace(np.nan, '')
    return df


# Scrapování pronájmu
url_pronajem = "https://www.bezrealitky.cz/vyhledat?offerType=PRONAJEM&estateType=BYT"
response_pronajem = requests.get(url_pronajem)
soup_pronajem = bs(response_pronajem.content, "html.parser")

page_links_pronajem = soup_pronajem.find_all('a', class_='page-link')
last_page_pronajem = max([int(link.text)
                         for link in page_links_pronajem if link.text.isdigit()])

max_pages_pronajem = last_page_pronajem
links_pronajem = vsechny_odkazy_pronajmu(
    url_pronajem, max_pages_pronajem)

dataframes_pronajem = []
with tqdm(total=len(links_pronajem), desc="Scraping pronájem data") as pbar_pronajem:
    for index_pronajem, link_pronajem in enumerate(links_pronajem, start=1):
        df_pronajem = scrape_data_pronajmu(
            link_pronajem, index_pronajem, "Pronájem")
        dataframes_pronajem.append(df_pronajem)
        pbar_pronajem.update(1)

merged_df_pronajem = pd.concat(dataframes_pronajem)

snapshot_date_pronajem = datetime.date.today().strftime("%Y-%m-%d")

merged_df_pronajem.insert(0, "Snapshot", snapshot_date_pronajem)

merged_df_pronajem = merged_df_pronajem.fillna('Neposkytují informace')

# Scrapování prodeje
url_prodej = "https://www.bezrealitky.cz/vyhledat?offerType=PRODEJ&estateType=BYT"
response_prodej = requests.get(url_prodej)
soup_prodej = bs(response_prodej.content, "html.parser")

# Získání poslední stránky prodeje
page_links_prodej = soup_prodej.find_all('a', class_='page-link')
last_page_prodej = max([int(link.text)
                        for link in page_links_prodej if link.text.isdigit()])

max_pages_prodej = last_page_prodej

# Získání odkazů na všechny stránky prodeje
links_prodej = vsechny_odkazy_prodej(
    url_prodej, max_pages_prodej)

dataframes_prodej = []
# tqdm poskytuje jednoduché a elegantní rozhraní pro vytváření průběžného (progress) ukazatele (progress bar) při iteraci nebo zpracování dat.
with tqdm(total=len(links_prodej), desc="Scraping data prodeje") as pbar_prodej:
    # enumerate projde prvky seznamu a zárověň získat jejich pořádí (index)
    for index_prodej, link_prodej in enumerate(links_prodej, start=1):
        try:
            # Scrapování dat prodeje z každého odkazu
            df_prodej = scrape_data_prodej(link_prodej, index_prodej)
            dataframes_prodej.append(df_prodej)
            pbar_prodej.update(1)
        # Exception = používají k označení a správě chyb nebo výjimečných situací v programu.
        except Exception as e:
            print(f"Error scraping data for link {link_prodej}: {e}")

# Sloučení všech DataFrame prodeje do jednoho
merged_df_prodej = pd.concat(dataframes_prodej)

# Přidání sloupce s datem snímku
snapshot_date_prodej = datetime.date.today().strftime("%Y-%m-%d")
merged_df_prodej.insert(0, "Snapshot", snapshot_date_prodej)

# Nahrazení chybějících hodnot řetězcem "Neposkytují informace"
merged_df_prodej = merged_df_prodej.fillna('Neposkytují informace')

# Sloučení DataFrame prodeje a pronájmu
merged_df = pd.concat([merged_df_prodej, merged_df_pronajem])

# Vytvoření výstupního adresáře
output_dir = "data"
# Tento kód vytvoří adresář s názvem "data". Pokud adresář již existuje, kód pokračuje bez chyby.
os.makedirs(output_dir, exist_ok=True)
# Vytvoření názvu výstupního souboru
snapshot_date = datetime.date.today().strftime("%Y-%m-%d")
output_file = os.path.join(output_dir, f"data_{snapshot_date}.csv")
# Nahrazení chybějících hodnot řetězcem "Neposkytují informace"
merged_df = merged_df.fillna('Neposkytují informace')
# Resetování indexu DataFrame a přičtení 1 ke každému indexu
merged_df.reset_index(drop=True, inplace=True)
merged_df.index = merged_df.index + 1

# Uložení DataFrame do CSV souboru
merged_df.to_csv(output_file, index=False)
print(f"Data uložena do {output_file}")
merged_df
