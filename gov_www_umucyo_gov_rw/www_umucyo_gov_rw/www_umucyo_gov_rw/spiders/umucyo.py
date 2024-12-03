import os
import pandas as pd
import scrapy
from scrapy.cmdline import execute
from parsel import Selector
from datetime import datetime
from io import StringIO
import unidecode


def remove_extra_space(column):
    # Remove any extra spaces or newlines created by this replacement
    column = column.replace(r'\s+', ' ', regex=True)
    column = column.str.strip()
    # Update the cleaned value back in row_data
    return column

def remove_specific_punctuation(text):
    punctuation_marks = [
        ".", ",", "?", "!", ":", "\n", "\t", ";", "—", "-", "'", '"', "(", ")", "[", "]", "{", "}", "…", "\\", "@", "&", "*",
        "_", "^", "~","`"
    ]
    for char in punctuation_marks:
        text = str(text).replace(char, '')  # Replace each punctuation mark with an empty string
    return text

def unidecode_converter(text):
    return unidecode.unidecode(text)

def date_of_birth_fun(text):
    try:
        date_of_birth = datetime.strptime(text, '%d/%m/%Y').strftime('%Y-%m-%d')
    except:
        date_of_birth = 'N/A'
    return date_of_birth

class UmucyoSpider(scrapy.Spider):
    name = "umucyo"
    start_urls = ["https://www.umucyo.gov.rw/"]

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,tr;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.umucyo.gov.rw',
        'Pragma': 'no-cache',
        'Referer': 'https://www.umucyo.gov.rw/um/ubl/moveUmUblBlacLstComListPubBlacklisted.do',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    body = 'menuId=&isBack=&currentPageNo=1&langCode=en&searchConditions=&chifNid=&seqno=&tin=&reportFilePath=https%3A%2F%2Fwww.umucyo.gov.rw%2Freport%2FUMUBLBlaclstLstRpt.crf&reportViewUrl=https%3A%2F%2Fwww.umucyo.gov.rw%2Frt%2FCmReportView.jsp&reportSystem=um&reportFileName=Black-list_List&searchSuplrNm=&fromSanctDt=&fromSanctDtBtw=&endSanctDt=&endSanctDtBtw=&recordCountPerPage=1000'

    def start_requests(self):
        yield scrapy.Request(
            'https://www.umucyo.gov.rw/um/ubl/moveUmUblBlacLstComListPubBlacklisted.do',
            method='POST',
            dont_filter=True,
            headers=self.headers,
            body=self.body,
        )

    def parse(self, response, **kwargs):
        text_converted_in_unicode = response.text
        selector = Selector(text_converted_in_unicode)

        data = selector.xpath('//table[@class="article_table mb10"]').get()

        html_content = StringIO(data)
        df = pd.read_html(html_content)[0]

        # Drop unnamed columns
        df = df.loc[:, ~df.columns.str.contains('Unnamed')]

        # Replace empty strings with 'N/A'
        df = df.replace('', 'N/A').replace('None', 'N/A')
        df.fillna('N/A', inplace=True)
        df.insert(0, 'id', range(1, len(df) + 1))

        # Convert column headers to lowercase and replace spaces with underscores
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        df.drop('no.', axis=1, inplace=True)
        if not os.path.exists('files'):
            os.makedirs('files')
        df['url'] = 'https://www.umucyo.gov.rw/um/ubl/moveUmUblBlacLstComListPubBlacklisted.do'
        df['company'] = df['company'].apply(remove_specific_punctuation)
        df['company'] = remove_extra_space(df['company'])
        df['owner'] = remove_extra_space(df['owner'])
        df['owner'] = df['owner'].apply(unidecode_converter)
        df['reason'] = remove_extra_space(df['reason'])
        df['start_date'] = df['start_date'].apply(date_of_birth_fun)
        df['end_date'] = df['end_date'].apply(date_of_birth_fun)
        priority_columns = ["id", "url"]
        df = df[priority_columns + [col for col in df.columns if col not in priority_columns]]

        input_file_path = os.getcwd() + f"\\files\\umucyo_{datetime.now().strftime('%Y%m%d')}.xlsx"
        df.to_excel(input_file_path, index=False)

if __name__ == '__main__':
    # execute("scrapy crawl kia".split())x`
    execute("scrapy crawl umucyo".split())
