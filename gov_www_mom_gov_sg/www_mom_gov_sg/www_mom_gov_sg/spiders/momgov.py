import json
import os
from typing import Union, Iterable
import pandas as pd
import scrapy
from scrapy import Spider, Request
from scrapy.cmdline import execute
from parsel import Selector
from datetime import datetime

class MomgovSpider(scrapy.Spider):
    name = "momgov"
    start_urls = ["https://www.mom.gov.sg/employment-practices/employers-convicted-under-employment-act#/"]

    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9,tr;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://www.mom.gov.sg/employment-practices/employers-convicted-under-employment-act",
        "sec-ch-ua": "\"Google Chrome\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest"
    }

    cookies = {
        "visid_incap_130760": "6PSDY9DvQ0ucw1dMOkLH/C9NSWcAAAAAQUIPAAAAAACIKytLfOAc+KrsYac343u2",
        "_gcl_au": "1.1.1884261178.1732857140",
        "suid": "f32c58f1-fc96-4572-8302-ab0d2c69f4eb",
        "mom-onboarding-shown": "yes",
        "_hjSessionUser_3281924": "eyJpZCI6ImE0MDk0OTQxLTMzNzYtNTkyMS1hYWZjLTg5NDUwY2RkMDIzZiIsImNyZWF0ZWQiOjE3MzI4NTcxNDI5MzAsImV4aXN0aW5nIjp0cnVlfQ==",
        "shell#lang": "en",
        "ASP.NET_SessionId": "ftupppbvimvkkpeyspes1vp0",
        "pgle_cururl": "http://www.mom.gov.sg/employment-practices/employers-convicted-under-employment-act",
        "pgle_curitem": "3f1ab9da-744b-4817-aa1e-bdf76bc34cb3",
        "nlbi_130760": "VrRxMFE3pQjACjn87ptQtgAAAACQLKnq5pAU8k1IQX4+ykI6",
        "incap_ses_996_130760": "A821NEcrWRQa3x4C8YDSDWTZT2cAAAAAEjNqy5zYpGpszzXrG0DjBg==",
        "SC_ANALYTICS_GLOBAL_COOKIE": "0a4c9856c16649d7b6762735935ba00f|True",
        "_sp_ses.4f67": "*",
        "_gid": "GA1.3.559436905.1733286252",
        "_gaclientid": "2790250.1732857141",
        "_gasessionid": "20241204|00846717",
        "_hjSession_3281924": "eyJpZCI6ImU2ZTljYWViLTU3OWMtNDJjMi04MWI2LTczMjFlY2UzZTA3ZSIsImMiOjE3MzMyODYyNTQ1MDgsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=",
        "incap_ses_1598_130760": "/aC9M2qLL117C+AttzwtFpTbT2cAAAAAk36COi+zR0XHeUpPVl1WbA==",
        "pgle_srttime": "12/4/2024 12:33:27 PM",
        "_ga_59DNK15E1M": "GS1.3.1733286324.2.1.1733286810.0.0.0",
        "_ga_D5C6SJ8G5Y": "GS1.1.1733286257.2.1.1733286810.0.0.0",
        "_sp_id.4f67": "fcdc1bca-945e-4237-bde2-7d861169b306.1732857140.2.1733286814.1732857286.2cc364de-b72d-4ecc-a675-6d9ca3e14d9d.fad494e9-e09f-4764-8a46-ffa34fd2665a.2d17d7d4-dcda-45de-b1da-40a007c82ccc.1733286256922.1",
        "_gahitid": "10:03:34",
        "_gat_UA-12831763-1": "1",
        "_ga_JLTM8R4V4E": "GS1.1.1733286250.2.1.1733286814.56.0.0",
        "_ga": "GA1.1.2790250.1732857141"
    }

    def start_requests(self):
        url = 'https://www.mom.gov.sg/api/v2/Rows?app_name=employers-convicted-employment-act&per_page=1000&page=1&order=&orderby=&q=&contentType=application%2Fjson%3B%20charset%3Dutf-8&dataType=json&crossDomain=true'

        yield scrapy.Request(
                                url=url,
                                method='GET',
                                dont_filter=True,
                                cookies=self.cookies,
                                headers=self.headers,
                                callback=self.parse
                            )
    def parse(self, response, **kwargs):
        # print(response.text)
        # print(response.status)
        json_data = json.loads(response.text)
        json_data = json_data['response']['rows']

        print(json_data)

if __name__ == '__main__':
    # execute("scrapy crawl kia".split())x`
    execute("scrapy crawl momgov".split())

