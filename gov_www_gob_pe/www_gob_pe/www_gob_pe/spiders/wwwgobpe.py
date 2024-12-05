import json
import os
import time
from typing import Union, Iterable
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

def translate_text(text, source_lang, target_lang):
    """
    Translate a single text string using GoogleTranslator.
    """
    gtrans_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,tr;q=0.8',
        'cache-control': 'no-cache',
        # 'cookie': 'SID=g.a000qgiE86cf_U8KuoN--kRfyGgj5e43jRGX8cJiAmoXnaEBL_ECSyzexV433Yv6MecacKkfsQACgYKAZMSARASFQHGX2Mi9x0D4EiJ3Xbqpi6XD83CEBoVAUF8yKq023dV85gPXppFIFdapnmM0076; __Secure-1PSID=g.a000qgiE86cf_U8KuoN--kRfyGgj5e43jRGX8cJiAmoXnaEBL_ECyg2R0C5kppdjciJAJ9-gFQACgYKAfISARASFQHGX2MiP6rDWyzw7aqaZpyOTPdbPRoVAUF8yKomuo7DrXKXgzqJgh4coOM70076; __Secure-3PSID=g.a000qgiE86cf_U8KuoN--kRfyGgj5e43jRGX8cJiAmoXnaEBL_ECqMEY5u4-tLn84Lv61_N74QACgYKAZgSARASFQHGX2MiwPQb0wjs9MC4c5KqJO00_BoVAUF8yKo-iURgwIzymWu4S7BjQNAA0076; HSID=Akfbc5PYN9P2V3c7d; SSID=A6dj94qEYtFLyaoyQ; APISID=BKPFhwlGmmdwwwK-/AuiXcEDG4M2gDHbYy; SAPISID=ElfTRU8GxnrF7Apo/AvufrDzlNgmcUylHr; __Secure-1PAPISID=ElfTRU8GxnrF7Apo/AvufrDzlNgmcUylHr; __Secure-3PAPISID=ElfTRU8GxnrF7Apo/AvufrDzlNgmcUylHr; SEARCH_SAMESITE=CgQI15wB; __Secure-ENID=24.SE=ZrjRl9yj4BoEFKOnsUM_myXdXzcaZ1H_EIGA7AA9CH6DXOi_TKBEVXkjqx5lzurZGUZyNS8wLBAxPIDut3WT3-fffSjH2MT7lAz8gw1ZSMmnmjtNrzMLkJaLlUTHV8c6c4IGtmpGLcS3Ge2c3bvQdvLYBzye4Vao4ZfpxheYYH5qXYR2TKKa3j8ajkM-zRSNgrpCw3Gde55qrSWDDpk4qOntcdKKIMXfd6EQhPjOsXYh_W_GPpfdOiJxYiWYEKbeGJqtFMxGzhqKpSfivbcQYB1w3_tlWD5nsAmesOf61xUqUSJC7QwjpXb3K4i5R1W1-kEuPDieDIaZE4Nsjtwj7VDlQEuvsDjAYEJpKT-ZuILwTHw4S4nt5fg; AEC=AZ6Zc-VVtOfvqbh_n35kiuMHJ2jbhCb3hhr1eHm_KQFRe2yX7CwULMMYIfc; OTZ=7847333_34_34__34_; NID=519=F6D3riUC5AN7VNFCEQTmwMGP4QZk36steLkSDW2_cdmPl8BGhPbzORegOgNJHiQJCZCkS4ycmeXO-4cUzR-bmJiAWMamrcTdKmpTQ_d55gL8kFaTSax769gyRC8NUFzl5bmiHvVdAXm7lGPk-wJqDWvKlS2x8ZA-7sHZU0VmNIoyYyZqVAuISo-fgfui6ZpQnmk6CRSSjHUXQlUzvEXOuaM_JVGo7vVGefOkg8jmYLMfy4GKcbHtDGANBnzjB6iZy523X4cGzzC1RxDAOCvigXmPDB4mnCwCqNGhbW9ekMKeTvEVdrK9LcvCCUgfAALQ2ZGwqt0sWo_XuMWb_mNN3JBQDJMNs5S1vKhhsY1R1Bw6BL2EOc9sTu6Pc6k05bEUMHzjK2O8iO1yjgk31M2ZtVCcL1MXWxrenBxwQryLS0VVVXyqtUiqO6_WZgFqXoIGrxeUV_-ToxktZb1zHgILePrLRbRsE72lBXoX9e-BydMpm91J3vIo2Gqp00ase02H1YNago9GGC7C5mcsh2p5kkb1N-HdORVkOZBqkDBcfjj3ONsdGWg9fxh1P1HZy26CR7qyDOa9shkartMfJ7L0ICriObzOh_ln4mIOyQ2KXWhGmH5Uv77OmS12ienDPSLys6E_BRGF2dW3iWcIOKksIhZ8jckya6eiIiZHa-TFsAhQni2d_rWEvY1jfyX70d0yfmtILIq2tskvecXoQVbM9dMML65zgcOsbV3fxrhiDSfLSYkaW9W04TkXHbDH-f0HKoB-Ci1OGZca52Txrpj2qdHvVQ; __Secure-1PSIDTS=sidts-CjEBQT4rX-pEUHgJw6U2xuPjj5PlYYtX-sndvcgJGHw3_sc9fvm7QXSPjOxJipd_zZquEAA; __Secure-3PSIDTS=sidts-CjEBQT4rX-pEUHgJw6U2xuPjj5PlYYtX-sndvcgJGHw3_sc9fvm7QXSPjOxJipd_zZquEAA; SIDCC=AKEyXzWW8y-5uWrM0VcCN0e6nGNgdhaC1LXfUXIQK1ilUNDhJeywBBjETJwRBrdAQM0K8vz15A; __Secure-1PSIDCC=AKEyXzXeRPU_m1The1I2UlWm0gkWWhdpYH_3Fx5YdyD9J1jnI8KS0rmzeKj6UanYAMixx6ri1Q; __Secure-3PSIDCC=AKEyXzUU6eRR-YjSHPSRS0hYbydpATGLurkudC4_7xzcJumljV2SEjTI23EyHhhHJum_WUfH6RU',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'referer': 'https://translate.google.com/m',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-form-factors': '"Desktop"',
        'sec-ch-ua-full-version': '"131.0.6778.86"',
        'sec-ch-ua-full-version-list': '"Google Chrome";v="131.0.6778.86", "Chromium";v="131.0.6778.86", "Not_A Brand";v="24.0.0.0"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-ch-ua-wow64': '?0',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'x-browser-channel': 'stable',
        'x-browser-copyright': 'Copyright 2024 Google LLC. All rights reserved.',
        'x-browser-validation': 'Nbt54E7jcg8lQ4EExJrU2ugNG6o=',
        'x-browser-year': '2024',
        'x-client-data': 'CIq2yQEIo7bJAQiKksoBCKmdygEIn5XLAQiUocsBCPaYzQEIhaDNAQj9pc4BCNWszgEI6rzOAQj9yc4BCMbPzgEIrtDOAQji0M4BCJzSzgEIi9POAQiy084B',
    }
    try:
        # Skip translation for NaN or empty values
        if pd.isna(text) or text == "":
            return text
        date = GoogleTranslator(source=source_lang, target=target_lang).translate(text)
        # params = {
        #     'sl': 'auto',
        #     'tl': 'en',
        #     'hl': 'en',
        #     'q': text,
        # }
        #
        # res = requests.get('https://translate.google.com/m',
        #                    params=params,
        #                    headers=gtrans_headers
        #                    )
        # selector = Selector(res.text)
        # date = selector.xpath('//div[@class="result-container"]//text()').get('N/A')
        return date
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

def date_extractor(text: str):
    try:
        text = text.split('-')[0].strip()
        date = datetime.strptime(text, '%B %d, %Y').strftime('%Y-%m-%d')
    except:
        try:
            text = text.split('-')[0].strip()
            date = datetime.strptime(text, '%d %B %Y').strftime('%Y-%m-%d')
        except:
            date = 'N/A'
    return date

class WwwgobpeSpider(scrapy.Spider):
    name = "wwwgobpe"
    allowed_domains = ["www.gob.pe"]
    start_urls = ["https://www.gob.pe/institucion/oefa/buscador?term=Sanction&institucion=oefa&topic_id=&contenido=noticias&sort_by=none"]

    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9,tr;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://www.gob.pe/institucion/oefa/buscador?contenido=noticias&institucion=oefa&sheet=7&sort_by=none&term=Sanction",
        "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }

    gtrans_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,tr;q=0.8',
        'cache-control': 'no-cache',
        # 'cookie': 'SID=g.a000qgiE86cf_U8KuoN--kRfyGgj5e43jRGX8cJiAmoXnaEBL_ECSyzexV433Yv6MecacKkfsQACgYKAZMSARASFQHGX2Mi9x0D4EiJ3Xbqpi6XD83CEBoVAUF8yKq023dV85gPXppFIFdapnmM0076; __Secure-1PSID=g.a000qgiE86cf_U8KuoN--kRfyGgj5e43jRGX8cJiAmoXnaEBL_ECyg2R0C5kppdjciJAJ9-gFQACgYKAfISARASFQHGX2MiP6rDWyzw7aqaZpyOTPdbPRoVAUF8yKomuo7DrXKXgzqJgh4coOM70076; __Secure-3PSID=g.a000qgiE86cf_U8KuoN--kRfyGgj5e43jRGX8cJiAmoXnaEBL_ECqMEY5u4-tLn84Lv61_N74QACgYKAZgSARASFQHGX2MiwPQb0wjs9MC4c5KqJO00_BoVAUF8yKo-iURgwIzymWu4S7BjQNAA0076; HSID=Akfbc5PYN9P2V3c7d; SSID=A6dj94qEYtFLyaoyQ; APISID=BKPFhwlGmmdwwwK-/AuiXcEDG4M2gDHbYy; SAPISID=ElfTRU8GxnrF7Apo/AvufrDzlNgmcUylHr; __Secure-1PAPISID=ElfTRU8GxnrF7Apo/AvufrDzlNgmcUylHr; __Secure-3PAPISID=ElfTRU8GxnrF7Apo/AvufrDzlNgmcUylHr; SEARCH_SAMESITE=CgQI15wB; __Secure-ENID=24.SE=ZrjRl9yj4BoEFKOnsUM_myXdXzcaZ1H_EIGA7AA9CH6DXOi_TKBEVXkjqx5lzurZGUZyNS8wLBAxPIDut3WT3-fffSjH2MT7lAz8gw1ZSMmnmjtNrzMLkJaLlUTHV8c6c4IGtmpGLcS3Ge2c3bvQdvLYBzye4Vao4ZfpxheYYH5qXYR2TKKa3j8ajkM-zRSNgrpCw3Gde55qrSWDDpk4qOntcdKKIMXfd6EQhPjOsXYh_W_GPpfdOiJxYiWYEKbeGJqtFMxGzhqKpSfivbcQYB1w3_tlWD5nsAmesOf61xUqUSJC7QwjpXb3K4i5R1W1-kEuPDieDIaZE4Nsjtwj7VDlQEuvsDjAYEJpKT-ZuILwTHw4S4nt5fg; AEC=AZ6Zc-VVtOfvqbh_n35kiuMHJ2jbhCb3hhr1eHm_KQFRe2yX7CwULMMYIfc; OTZ=7847333_34_34__34_; NID=519=F6D3riUC5AN7VNFCEQTmwMGP4QZk36steLkSDW2_cdmPl8BGhPbzORegOgNJHiQJCZCkS4ycmeXO-4cUzR-bmJiAWMamrcTdKmpTQ_d55gL8kFaTSax769gyRC8NUFzl5bmiHvVdAXm7lGPk-wJqDWvKlS2x8ZA-7sHZU0VmNIoyYyZqVAuISo-fgfui6ZpQnmk6CRSSjHUXQlUzvEXOuaM_JVGo7vVGefOkg8jmYLMfy4GKcbHtDGANBnzjB6iZy523X4cGzzC1RxDAOCvigXmPDB4mnCwCqNGhbW9ekMKeTvEVdrK9LcvCCUgfAALQ2ZGwqt0sWo_XuMWb_mNN3JBQDJMNs5S1vKhhsY1R1Bw6BL2EOc9sTu6Pc6k05bEUMHzjK2O8iO1yjgk31M2ZtVCcL1MXWxrenBxwQryLS0VVVXyqtUiqO6_WZgFqXoIGrxeUV_-ToxktZb1zHgILePrLRbRsE72lBXoX9e-BydMpm91J3vIo2Gqp00ase02H1YNago9GGC7C5mcsh2p5kkb1N-HdORVkOZBqkDBcfjj3ONsdGWg9fxh1P1HZy26CR7qyDOa9shkartMfJ7L0ICriObzOh_ln4mIOyQ2KXWhGmH5Uv77OmS12ienDPSLys6E_BRGF2dW3iWcIOKksIhZ8jckya6eiIiZHa-TFsAhQni2d_rWEvY1jfyX70d0yfmtILIq2tskvecXoQVbM9dMML65zgcOsbV3fxrhiDSfLSYkaW9W04TkXHbDH-f0HKoB-Ci1OGZca52Txrpj2qdHvVQ; __Secure-1PSIDTS=sidts-CjEBQT4rX-pEUHgJw6U2xuPjj5PlYYtX-sndvcgJGHw3_sc9fvm7QXSPjOxJipd_zZquEAA; __Secure-3PSIDTS=sidts-CjEBQT4rX-pEUHgJw6U2xuPjj5PlYYtX-sndvcgJGHw3_sc9fvm7QXSPjOxJipd_zZquEAA; SIDCC=AKEyXzWW8y-5uWrM0VcCN0e6nGNgdhaC1LXfUXIQK1ilUNDhJeywBBjETJwRBrdAQM0K8vz15A; __Secure-1PSIDCC=AKEyXzXeRPU_m1The1I2UlWm0gkWWhdpYH_3Fx5YdyD9J1jnI8KS0rmzeKj6UanYAMixx6ri1Q; __Secure-3PSIDCC=AKEyXzUU6eRR-YjSHPSRS0hYbydpATGLurkudC4_7xzcJumljV2SEjTI23EyHhhHJum_WUfH6RU',
        'pragma': 'no-cache',
        'priority': 'u=0, i',
        'referer': 'https://translate.google.com/m',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-form-factors': '"Desktop"',
        'sec-ch-ua-full-version': '"131.0.6778.86"',
        'sec-ch-ua-full-version-list': '"Google Chrome";v="131.0.6778.86", "Chromium";v="131.0.6778.86", "Not_A Brand";v="24.0.0.0"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-ch-ua-wow64': '?0',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'x-browser-channel': 'stable',
        'x-browser-copyright': 'Copyright 2024 Google LLC. All rights reserved.',
        'x-browser-validation': 'Nbt54E7jcg8lQ4EExJrU2ugNG6o=',
        'x-browser-year': '2024',
        'x-client-data': 'CIq2yQEIo7bJAQiKksoBCKmdygEIn5XLAQiUocsBCPaYzQEIhaDNAQj9pc4BCNWszgEI6rzOAQj9yc4BCMbPzgEIrtDOAQji0M4BCJzSzgEIi9POAQiy084B',
    }

    cookies = {
        '_ga': 'GA1.1.1510528277.1732859778',
        '_clck': '1a67gpb%7C2%7Cfrg%7C0%7C1794',
        '_ga_L4XC6VEB35': 'GS1.1.1733390585.3.1.1733391139.60.0.0',
        '_clsk': 'czddlj%7C1733391141062%7C4%7C0%7Ce.clarity.ms%2Fcollect',
        'aws-waf-token': '9d4f960c-c403-4673-9490-aefa6a46ca84:FAoAaQ9C4wMcAAAA:86yX4Z1Is27DmLbGDQ0DBDoBN+TDLqybRdtZb8ZkmbnQB660PFOeLGWVCc3r8uMmPL2AO81kFCYEMXpybDSApyAsMCXjahjXwT3O3PjWKBVo74jdBrc4z8EEjxeOVNN+HIBshNjV53DPpSHOhGXoZNjHgbVrZYW60YcIiWI1jS80WIuqS8AfVB1s2CjV',
        '_epic_alpha_session_key': '8P3zpkGf60SzekQeH8HJA0tk1A9zgHR9l6v1svszt%2FXSxeIBbwUmBbqK15zXF0xWJ97Gb3ejPznftmQNVEfijvB7TK2x4f%2FTlqH0JnDIfOS2B2CQok1hoOrWExSnoqeGsRIHSQHoBEdd64mbt%2BEUpLxzkrGGyyJH7oLeqEarJ1QUs%2F%2Bln07m4QEjYdc6bZ%2BxxxXkzTyaNOAEG3m1HkwxXW%2BgaPiRGRocbIJCymelh9WaGPKBVySC8RHYaVqh%2BjP86sqoY4rllFHdwhcIiXglSRzrwEChdT9Bz9qbuG3q3w%3D%3D--M4xXFk77Hn4ogdV2--ZD6CFl%2BwVvr5d7bEOpqsOA%3D%3D',
    }


    main_lis = []
    def start_requests(self):
        for posi in range(1, 11):
            url = f'https://www.gob.pe/busquedas.json?contenido=noticias&institucion=oefa&sheet={posi}&sort_by=none&term=Sanction'
            yield scrapy.Request(
                                url=url,
                                method='GET',
                                dont_filter=True,
                                cookies=self.cookies,
                                headers=self.headers,
                                callback=self.parse
                            )


    def parse(self, response, **kwargs):
        print(response.text)
        print(response.status)
        json_data = json.loads(response.text)
        for data in json_data['data']['attributes']['results']:
            url = 'https://www.gob.pe' + data['url'].split('href="')[-1].split('">')[0]
            date = data['publication']
            yield scrapy.Request(url,
                                 cb_kwargs={'date': date},
                                 callback=self.get_detail_data)

    def get_detail_data(self, response, **kwargs):


        main_dic = {}
        date = kwargs['date']
        params = {
            'sl': 'auto',
            'tl': 'en',
            'hl': 'en',
            'q': date,
        }

        # print(date)
        res = requests.get('https://translate.google.com/m',
                                params=params,
                                headers=self.gtrans_headers
                                )
        selector = Selector(res.text)
        date = selector.xpath('//div[@class="result-container"]//text()').get()
        # print(date)
        selector = Selector(response.text)
        title = selector.xpath('//h1[@class="text-3xl md:text-4xl leading-9 font-extrabold"]//text()').get()

        description = ' '.join(selector.xpath('//section[@class="body"]//text()').getall())
        main_dic['url'] = response.url
        main_dic['date'] = date
        main_dic['title'] = title
        main_dic['description'] = description

        self.main_lis.append(main_dic)

    def close(self, spider: Spider, reason: str):
        if not os.path.exists('files'):
            os.makedirs('files')
        df = pd.json_normalize(self.main_lis, sep="_")
        print(df)
        df = df.loc[:, ~df.columns.str.contains('Unnamed')]

        df['date'] = df['date'].apply(date_extractor)
        df['date'] = remove_extra_space(df['date'])
        df['title'] = remove_extra_space(df['title'])
        df['description'] = remove_extra_space(df['description'])

        # Replace empty strings with 'N/A'
        df = df.replace('', 'N/A').replace('None', 'N/A')
        df.fillna('N/A', inplace=True)
        df.insert(0, 'id', range(1, len(df) + 1))

        # Convert column headers to lowercase and replace spaces with underscores
        df.columns = df.columns.str.lower().str.replace(' ', '_')

        priority_columns = ["id", "url"]
        df = df[priority_columns + [col for col in df.columns if col not in priority_columns]]

        input_file_path = os.getcwd() + f"\\files\\gob_{datetime.now().strftime('%Y%m%d')}.xlsx"
        df.to_excel(input_file_path, index=False)

        source_language = "pe"  # Detect language peru
        target_language = "en"
        translated_df = translate_dataframe(df, source_language, target_language, max_workers=10)

        translated_df['title'] = remove_extra_space(translated_df['title'])
        translated_df['description'] = remove_extra_space(translated_df['description'])

        translated_df = translated_df.replace('', 'N/A').replace('None', 'N/A')  # Replace empty strings
        translated_df.fillna('N/A', inplace=True)  # Replace None or NaN with 'N/A'

        # sort the columns in a DataFrame
        sorted_columns = translated_df.columns.sort_values()
        translated_df = translated_df.reindex(columns=sorted_columns)

        # Sort the columns, prioritizing specific ones like "url", "description", "supplier_summary"
        priority_columns = ["id", "url"]
        translated_df = translated_df[
            priority_columns + [col for col in translated_df.columns if col not in priority_columns]]
        input_file_path = os.getcwd() + f"\\files\\translated_gob_{datetime.now().strftime('%Y%m%d')}.xlsx"

        # Export the DataFrame to Excel
        translated_df.to_excel(input_file_path, index=False, engine='openpyxl')


if __name__ == '__main__':
    # execute("scrapy crawl kia".split())x`
    execute("scrapy crawl wwwgobpe".split())