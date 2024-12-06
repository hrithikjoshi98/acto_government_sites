import json
import os
import random
import time
from typing import Union, Iterable
from urllib.parse import urljoin
import numpy as np
import pandas as pd
import requests
import scrapy
from scrapy import Spider, Request
from scrapy.cmdline import execute
from parsel import Selector
from datetime import datetime
from deep_translator import GoogleTranslator
from concurrent.futures import ThreadPoolExecutor
from twisted.internet.defer import Deferred

def remove_extra_space(column):
    # Remove any extra spaces or newlines created by this replacement
    column = column.replace(r'\s+', ' ', regex=True)
    column = column.str.strip()
    # Update the cleaned value back in row_data
    return column

def translate_text_with_retries(translator, value, max_retries=5):
    """Translate text with retry mechanism in case of errors."""
    retries = 0
    while retries < max_retries:
        try:
            # Attempt translation
            translated_value = translator.translate(text=value,)
            return translated_value
        except Exception as e:
            retries += 1
            wait_time = random.uniform(a=2, b=5) * retries  # Exponential backoff with some randomness
            print(f"Error translating '{value}': {e}. Retrying in {wait_time:.2f} seconds (Attempt {retries}/{max_retries})")
            time.sleep(wait_time)
    return value  # Return original value if all retries fail


def translate_chunk_rows(chunk, translator, columns):
    """Translate a chunk of rows in the dataframe for specified columns."""


    for index, row in chunk.iterrows():
        for col in columns:
            if col in chunk.columns:
                value = row[col]
                try:
                    if isinstance(value, str) and value.strip().lower() == 'N/A':
                        chunk.at[index, col] = 'N/A'
                    elif isinstance(value, str) and value.strip() != '':
                        # Translate the value with retries
                        translated_value = translate_text_with_retries(translator, value)
                        chunk.at[index, col] = translated_value
                        print(f"Row {index}, Col '{col}': Translated '{value}' -> '{translated_value}'")
                except Exception as e:
                    print(f"Error translating '{value}' in row {index}, column '{col}': {e}")
                    chunk.at[index, col] = value  # Keep original value if error occurs
    return chunk


def translate_dataframe_in_chunks(df, translator, columns, n_workers=10):
    """Helper function to translate specified columns in the dataframe using parallel processing."""
    chunks = np.array_split(df, n_workers)
    print(f"Dataframe split into {n_workers} chunks for parallel processing.")

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        results = list(executor.map(lambda chunk: translate_chunk_rows(chunk, translator, columns), chunks))
    print("All chunks processed.")
    return pd.concat(results)

def date_extractor(text: str):
    try:
        text = text.replace(' ', '').strip()
        date = datetime.strptime(text, '%Y.%m.%d').strftime('%Y-%m-%d')
    except:
        date = 'N/A'
    return date

def df_cleaning(df):
    # Drop Unnamed columns
    df = df.loc[:, ~df.columns.str.contains('Unnamed')]

    # Replace empty strings with 'N/A'
    df = df.replace('', 'N/A').replace('None', 'N/A')
    df.fillna('N/A', inplace=True)

    # Add id column
    df.insert(0, 'id', range(1, len(df) + 1))

    # Convert column headers to lowercase and replace spaces with underscores
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    return df

def prioritize_columns(df, priority_columns):
    df = df[priority_columns + [col for col in df.columns if col not in priority_columns]]
    return df

class TcontasSpider(scrapy.Spider):
    name = "tcontas"
    allowed_domains = ["www.tcontas.pt"]
    start_urls = ["https://www.tcontas.pt/pt-pt/ProdutosTC/Decisoes/Pages/Decisoes-do-Tribunal-de-Contas.aspx"]

    main_dic = {}
    main_list = []
    def parse(self, response, **kwargs):
        selector = Selector(response.text)

        decisions_of_court_list = selector.xpath('//div[@class="tc-item"]//a/@href').getall()
        for url in decisions_of_court_list:
            url = urljoin('https://www.tcontas.pt', url)
            print(url)
            yield scrapy.Request(
                url,
                callback=self.yearly_decisions
            )
    def yearly_decisions(self, response):
        selector = Selector(response.text)
        decisions_of_court_list = selector.xpath('//div[@class="tc-item"]')
        for data in decisions_of_court_list:
            url = data.xpath('.//a/@href').get('N/A')
            url = urljoin('https://www.tcontas.pt', url)
            print(url)
            if '.pdf' in url:
                self.main_dic = {}
                title = ' '.join(data.xpath('.//header//a//text()').getall())
                pdf_url = data.xpath('.//header//a/@href').get('N/A')
                pdf_url = urljoin('https://www.tcontas.pt', pdf_url)
                date = ''.join(
                    data.xpath('.//footer//span[@class="tc-item-info"]/span[@class="tc-date"]//text()').getall())
                information = ' '.join(data.xpath('.//footer//span[@class="tc-item-info"]/span[@class="tc-info"]//text()').getall())
                description = ' '.join(data.xpath('.//p//text()').getall())
                url = response.url

                self.main_dic['url'] = url
                self.main_dic['title'] = title
                self.main_dic['pdf_url'] = pdf_url
                self.main_dic['date'] = date
                self.main_dic['information'] = information
                self.main_dic['description'] = description
                self.main_list.append(self.main_dic)
            else:
                yield scrapy.Request(
                    url,
                    callback=self.detailed_data
                )

    def detailed_data(self, response):
        selector = Selector(response.text)

        data_list = selector.xpath('//div[@class="tc-item"]')
        for data in data_list:
            self.main_dic = {}
            title = ' '.join(data.xpath('.//header//a//text()').getall())
            pdf_url = data.xpath('.//header//a/@href').get('N/A')
            pdf_url = urljoin('https://www.tcontas.pt', pdf_url)
            date = ''.join(data.xpath('.//footer//span[@class="tc-item-info"]/span[@class="tc-date"]//text()').getall())
            information = ' '.join(data.xpath('.//footer//span[@class="tc-item-info"]/span[@class="tc-info"]//text()').getall())
            description = ' '.join(data.xpath('.//p//text()').getall())
            url = response.url

            self.main_dic['url'] = url
            self.main_dic['title'] = title
            self.main_dic['pdf_url'] = pdf_url
            self.main_dic['date'] = date
            self.main_dic['information'] = information
            self.main_dic['description'] = description
            self.main_list.append(self.main_dic)

    def close(self, spider: Spider, reason: str):
        os.makedirs('files', exist_ok=True)
        df = pd.DataFrame(self.main_list)

        df['date'] = remove_extra_space(df['date'])
        df['date'] = df['date'].apply(date_extractor)
        df['title'] = remove_extra_space(df['title'])
        df['pdf_url'] = remove_extra_space(df['pdf_url'])
        df['information'] = remove_extra_space(df['information'])
        df['description'] = remove_extra_space(df['description'])

        df = df_cleaning(df)

        priority_columns = ["id", "url"]
        df = prioritize_columns(df, priority_columns)

        input_file_path = os.getcwd() + f"\\files\\tcontas_{datetime.now().strftime('%Y%m%d')}.xlsx"
        df.to_excel(input_file_path, index=False)

        columns_to_translate = ["title", "information", 'description']
        # Perform chunked translation with parallel processing
        translator = GoogleTranslator(source='pt', target='en')
        translated_df = translate_dataframe_in_chunks(df, translator, columns_to_translate)

        df['date'] = remove_extra_space(df['date'])
        df['title'] = remove_extra_space(df['title'])
        df['pdf_url'] = remove_extra_space(df['pdf_url'])
        df['information'] = remove_extra_space(df['information'])
        df['description'] = remove_extra_space(df['description'])

        input_file_path = os.getcwd() + f"\\files\\new_translated_tcontas_{datetime.now().strftime('%Y%m%d')}.xlsx"

        # Export the DataFrame to Excel
        translated_df.to_excel(input_file_path, index=False, engine='openpyxl')


if __name__ == '__main__':
    # execute("scrapy crawl kia".split())x`
    execute("scrapy crawl tcontas".split())