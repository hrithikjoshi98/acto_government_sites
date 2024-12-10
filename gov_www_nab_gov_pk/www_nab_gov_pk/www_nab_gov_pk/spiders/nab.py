import os
from urllib.parse import urljoin
import pandas as pd
import scrapy
from scrapy import Spider
from scrapy.cmdline import execute
from datetime import datetime
import re

def find_penalty_sentences_and_amount(text):
    # Define the keywords to search for in the text
    keywords = r'\b(penalty|penalti|penalties|fine|fines|fined)\b'
    # Split text into sentences using a basic regex for sentence end punctuation
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)
    # Find sentences that contain the keywords
    penalty_sentences = [sentence for sentence in sentences if re.search(keywords, sentence, re.IGNORECASE)]
    # pattern = r'(Rs.)\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(million|billion|trillion|quadrillion|quintillion)?'
    pattern = r'(Rs?.)\s*(\d{1,9}(?:,\s*\d{1,9})*(?:\.\d+)?)\s*(Crore|crore|Million|million|Billion|billion|Trillion|trillion|Lakh|lakh|Thousand|thousand)?'
    # List to store matched amounts
    penalty_amounts = []
    # Search for monetary amounts in each sentence
    if penalty_sentences == []:
        return 'N/A'
    for sentence in penalty_sentences:
        matches = re.findall(pattern, sentence)
        if matches:
            # Format and add matches to the list
            for match in matches:
                # Join components of the match: currency symbol, number, and large unit (if any)
                amount = match[0] + match[1]
                if match[2]:  # If a large unit like 'million' is present, add it
                    amount += f' {match[2]}'
                penalty_amounts.append(amount)
    penalty_amounts = '|'.join(penalty_amounts)
    return penalty_amounts


def remove_extra_space(column):
    # Remove any extra spaces or newlines created by this replacement
    column = column.replace(r'\s+', ' ', regex=True)
    column = column.str.strip()
    # Update the cleaned value back in row_data
    return column

def date_extractor(text: str):
    try:
        text = text.split(':')[-1].strip()
        date = datetime.strptime(text, '%d-%b-%Y').strftime('%Y-%m-%d')
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
        "_", "^", "~", "`"
    ]
    for char in punctuation_marks:
        text = str(text).replace(char, '')  # Replace each punctuation mark with an empty string
    return text

def title_cleaning(text: str):
    try:
        text = text.replace('Title:', '').strip()
        return text
    except:
        return text

def url_normalization(url: str):
    if url != '':
        return urljoin('https://www.fsrc.kn', url)
    else:
        return 'N/A'


class NabSpider(scrapy.Spider):
    name = "nab"
    start_urls = ["https://www.nab.gov.pk/press/press_release2.asp?curpage=1"]

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9,tr;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }

    cookies = {
        "cookiesession1": "678B2994F9BB3EC9EB8EE3B73BB89580",
        "ASPSESSIONIDCAQDASSQ": "MDLDHMBDJKKDPDCOHNIEFGOK"
    }

    main_list = []

    def start_requests(self):
        yield scrapy.Request(
            self.start_urls[0],
            headers=self.headers,
            cookies=self.cookies,
            dont_filter=True,
            callback=self.parse
        )

    def parse(self, response, **kwargs):
        # detailed_page_url = 'https://www.nab.gov.pk/press/new.asp?2023'
        # yield scrapy.Request(
        #         detailed_page_url,
        #         dont_filter=True,
        #         headers=self.headers,
        #         cookies=self.cookies,
        #         callback=self.detail_page
        #     )
        press_releases_list = response.xpath('//a[contains(@href, "new.asp")]/@href').getall()
        for detailed_page_url in press_releases_list:
            detailed_page_url = urljoin('https://www.nab.gov.pk/press/', detailed_page_url)
            yield scrapy.Request(
                detailed_page_url,
                dont_filter=True,
                headers=self.headers,
                cookies=self.cookies,
                callback=self.detail_page
            )

        next_page_link = response.xpath('//input[@value="NEXT"]/@onclick').get()
        if next_page_link is not None:
            next_page_link = 'https://www.nab.gov.pk/press/' + next_page_link.split("'")[-2]
            print(next_page_link)
            yield scrapy.Request(
                next_page_link,
                dont_filter=True,
                headers=self.headers,
                cookies=self.cookies,
            )

    def detail_page(self, response):
        table_tr = response.xpath('//table[@id="table2"]//tr')
        main_dic = {}
        for pos, tr in enumerate(table_tr):
            data = ' '.join(tr.xpath('.//text()').getall())
            if pos == 0:
                main_dic['date'] = data
            if pos == 1:
                main_dic['title'] = data
            if pos == 2:
                main_dic['description'] = data
        main_dic['url'] = response.url
        self.main_list.append(main_dic)

    def close(self, spider: Spider, reason: str):
        os.makedirs('files', exist_ok=True)
        df = pd.DataFrame(self.main_list)

        df['date'] = remove_extra_space(df['date'])
        df['date'] = df['date'].apply(date_extractor)
        df['title'] = remove_extra_space(df['title'])
        df['title'] = df['title'].apply(title_cleaning)
        df['description'] = remove_extra_space(df['description'])
        df['penalty'] = df['description'].apply(find_penalty_sentences_and_amount)

        df = df_cleaning(df)

        priority_columns = ["id", "url"]
        df = prioritize_columns(df, priority_columns)

        input_file_path = os.getcwd() + f"\\files\\nab_{datetime.now().strftime('%Y%m%d')}.xlsx"
        df.to_excel(input_file_path, index=False)

if __name__ == '__main__':
    # execute("scrapy crawl kia".split())x`
    execute("scrapy crawl nab".split())



