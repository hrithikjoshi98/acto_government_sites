import os
from typing import Union
import pandas as pd
import scrapy
from scrapy import Spider
from scrapy.cmdline import execute
from parsel import Selector
from datetime import datetime
from io import StringIO
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor


def remove_extra_space(column):
    # Remove any extra spaces or newlines created by this replacement
    column = column.replace(r'\s+', ' ', regex=True)
    column = column.str.strip()
    # Update the cleaned value back in row_data
    return column

def translate_text(text, source_lang, target_lang):
    """
    Translate a single text string using GoogleTranslator.
    """
    try:
        # Skip translation for NaN or empty values
        if pd.isna(text) or text == "":
            return text
        return GoogleTranslator(source=source_lang, target=target_lang).translate(text)
    except Exception as e:
        print(f"Error translating '{text}': {e}")
        return text  # Return the original text in case of error


def translate_dataframe(df, source_lang, target_lang, max_workers=5):
    """
    Translate all rows and columns in a DataFrame using multithreading.
    """
    translated_data = pd.DataFrame()

    # Translate column names
    translated_columns = [
        translate_text(col, source_lang, target_lang) for col in df.columns
    ]
    df.columns = translated_columns  # Apply translated column names

    # Multithreading for faster translation of rows
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for column in df.columns:
            # Translate each column independently
            print(f"Translating column: {column}")
            translated_column = list(executor.map(lambda x: translate_text(x, source_lang, target_lang),
                df[column]
            ))
            translated_data[column] = translated_column

    return translated_data


def remove_specific_punctuation(text):
    punctuation_marks = [
        ".", ",", "?", "!", ":", "\n", "\t", ";", "—", "-", "'", '"', "(", ")", "[", "]", "{", "}", "…", "\\", "@", "&", "*",
        "_", "^", "~","`"
    ]
    for char in punctuation_marks:
        text = str(text).replace(char, '')  # Replace each punctuation mark with an empty string
    return text

def remove_single_space(text):
    return str(text).replace(' ', '')

class SupersegurosSpider(scrapy.Spider):
    name = "superseguros"
    start_urls = ["https://www.superseguros.gob.pa/sancion/companias-de-seguros/"]

    def parse(self, response, **kwargs):
        selector = Selector(response.text)
        if not os.path.exists('files'):
            os.makedirs('files')

        list_of_tables = selector.xpath('//table[contains(@id, "tablepress")]').getall()
        df_list = []
        for p, table in enumerate(list_of_tables):
            html_content = StringIO(table)
            df = pd.read_html(html_content)[0]
            # Drop unnamed columns
            df = df.loc[:, ~df.columns.str.contains('Unnamed')]

            # Replace empty strings with 'N/A'
            df = df.replace('', 'N/A').replace('None', 'N/A')
            df.fillna('N/A', inplace=True)

            try:
                val = df['ARTÍCULO']
                df = df.rename(columns={'ARTÍCULO': 'DISPOSICIÓN LEGAL INFRINGIDA'})
            except:
                pass
            try:
                val = df['TITULO DE LA FALTA']
                df = df.rename(columns={'TITULO DE LA FALTA': 'DESCRIPCIÓN DE LA FALTA'})
            except:
                pass
            # Convert column headers to lowercase and replace spaces with underscores
            df.columns = df.columns.str.lower().str.replace(' ', '_')
            df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)
        df['url'] = response.url
        df['denominación'] = df['denominación'].apply(remove_specific_punctuation)
        df['denominación'] = remove_extra_space(df['denominación'])
        df['resolución'] = remove_extra_space(df['resolución'])
        df['monto'] = df['monto'].apply(remove_single_space)
        df['monto'] = remove_extra_space(df['monto'])
        df['disposición_legal_infringida'] = remove_extra_space(df['disposición_legal_infringida'])
        df['descripción_de_la_falta'] = remove_extra_space(df['descripción_de_la_falta'])

        df.insert(0, 'id', range(1, len(df) + 1))

        priority_columns = ["id", "url"]
        df = df[priority_columns + [col for col in df.columns if col not in priority_columns]]
        input_file_path = os.getcwd() + f"\\files\\superseguros_{datetime.now().strftime('%Y%m%d')}.xlsx"
        df.to_excel(input_file_path, index=False)

        source_language = "es"  # Detect language spanish
        target_language = "en"
        translated_df = translate_dataframe(df, source_language, target_language, max_workers=10)

        translated_df = translated_df.replace('', 'N/A').replace('None', 'N/A')  # Replace empty strings
        translated_df.fillna('N/A', inplace=True)  # Replace None or NaN with 'N/A'

        # sort the columns in a DataFrame
        sorted_columns = translated_df.columns.sort_values()
        translated_df = translated_df.reindex(columns=sorted_columns)

        # Sort the columns, prioritizing specific ones like "url", "description", "supplier_summary"
        priority_columns = ["id", "url"]
        translated_df = translated_df[
            priority_columns + [col for col in translated_df.columns if col not in priority_columns]]
        input_file_path = os.getcwd() + f"\\files\\translated_superseguros_{datetime.now().strftime('%Y%m%d')}.xlsx"

        # Export the DataFrame to Excel
        translated_df.to_excel(input_file_path, index=False, engine='openpyxl')



if __name__ == '__main__':
    # execute("scrapy crawl kia".split())x`
    execute("scrapy crawl superseguros".split())
