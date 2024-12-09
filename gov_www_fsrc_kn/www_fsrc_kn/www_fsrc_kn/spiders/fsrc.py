import os
from urllib.parse import urljoin
import pandas as pd
import scrapy
from scrapy import Spider
from scrapy.cmdline import execute
from parsel import Selector
from datetime import datetime


def remove_extra_space(column):
    # Remove any extra spaces or newlines created by this replacement
    column = column.replace(r'\s+', ' ', regex=True)
    column = column.str.strip()
    # Update the cleaned value back in row_data
    return column

def date_extractor(text: str):
    try:
        text = text.split(':')[-1].strip()
        date = datetime.strptime(text, '%d %B %Y').strftime('%Y-%m-%d')
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

def remove_specific_punctuation(text):
    punctuation_marks = [
        ".", ",", "?", "!", ":", "\n", "\t", ";", "—", "-", "'", '"', "(", ")", "[", "]", "{", "}", "…", "\\", "@", "&", "*",
        "_", "^", "~","`"
    ]
    for char in punctuation_marks:
        text = str(text).replace(char, '')  # Replace each punctuation mark with an empty string
    return text

def url_normalization(url: str):
    if url != '':
        return urljoin('https://www.fsrc.kn', url)
    else:
        return 'N/A'

class FsrcSpider(scrapy.Spider):
    name = "fsrc"
    allowed_domains = ["www.fsrc.kn"]
    start_urls = ["https://www.fsrc.kn/warnings"]


    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9,tr;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://www.fsrc.kn",
        "Pragma": "no-cache",
        "Referer": "https://www.fsrc.kn/warnings",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }

    cookies = {
        "_ga": "GA1.2.1290697614.1732856432",
        "9e7ac264f51aaccdec61120f27e0e8d5": "73e32280598b5960d9cddfee9d4fd8cb",
        "_gid": "GA1.2.52828034.1733717700",
        "_gat": "1"
    }

    body = 'limit=0&filter_order=&filter_order_Dir=&limitstart=&task='
    main_list = []
    def start_requests(self):
        yield scrapy.Request(
                                url=self.start_urls[0],
                                method='POST',
                                dont_filter=True,
                                cookies=self.cookies,
                                headers=self.headers,
                                body=self.body,
                                callback=self.parse
                            )

    def parse(self, response, **kwargs):
        selector = Selector(response.text)

        list_of_warnings = selector.xpath('//td[@headers="categorylist_header_title"]/a/@href').getall()
        for warning_url in list_of_warnings:
            warning_url = urljoin('https://www.fsrc.kn', warning_url)
            yield scrapy.Request(
                warning_url,
                method='GET',
                dont_filter=True,
                cookies=self.cookies,
                headers=self.headers,
                callback=self.detail_page
            )

    def detail_page(self, response):
        main_dic = {}
        selector = Selector(response.text)
        name = selector.xpath('//h2[@itemprop="headline"]/text()').get('N/A')
        date = selector.xpath('//time[@itemprop="datePublished"]/text()').get('N/A')
        description = ' '.join(selector.xpath('//div[@itemprop="articleBody"]//text()').getall())
        additional_urls = '|'.join(selector.xpath('//div[@itemprop="articleBody"]/a//@href').getall())

        main_dic['name'] = name
        main_dic['date'] = date
        main_dic['description'] = description
        main_dic['additional_urls'] = additional_urls
        main_dic['url'] = response.url

        self.main_list.append(main_dic)

    def close(self, spider: Spider, reason: str):
        os.makedirs('files', exist_ok=True)
        df = pd.DataFrame(self.main_list)

        df['date'] = remove_extra_space(df['date'])
        df['date'] = df['date'].apply(date_extractor)
        df['name'] = remove_extra_space(df['name'].apply(remove_specific_punctuation))
        df['additional_urls'] = remove_extra_space(df['additional_urls'])
        df['additional_urls'] = df['additional_urls'].apply(url_normalization)
        df['description'] = remove_extra_space(df['description'])

        df = df_cleaning(df)

        priority_columns = ["id", "url"]
        df = prioritize_columns(df, priority_columns)

        input_file_path = os.getcwd() + f"\\files\\fsrc_{datetime.now().strftime('%Y%m%d')}.xlsx"
        df.to_excel(input_file_path, index=False)


if __name__ == '__main__':
    # execute("scrapy crawl kia".split())x`
    execute("scrapy crawl fsrc".split())


