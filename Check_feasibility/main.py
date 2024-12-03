import requests
import time
import pandas as pd


def check_url(url):
    try:
        print(f"Checking URL: {url}")
        start_time = time.time()
        response = requests.get(url, timeout=10)
        elapsed_time = time.time() - start_time

        # Collect basic information
        status_code = response.status_code
        headers = response.headers
        content_length = len(response.content)
        content_type = headers.get('Content-Type', 'Unknown')

        # Check for anti-bot measures
        anti_bot_signs = {
            "captcha": "captcha" in response.text.lower(),
            "robots.txt": "robots.txt" in response.url.lower(),
            "csrf_token": "csrf" in response.text.lower(),
            "cloudflare": "cf-ray" in headers,
        }

        print(f"Status Code: {status_code}")
        print(f"Response Time: {elapsed_time:.2f} seconds")
        print("Anti-Bot Measures Detected:")
        for key, value in anti_bot_signs.items():
            print(f"  - {key}: {'Yes' if value else 'No'}")
        print("-" * 40)

        # Return structured results
        return {
            "url": url,
            "status_code": status_code,
            "response_time": elapsed_time,
            "content_length": content_length,
            "content_type": content_type,
            "captcha_detected": anti_bot_signs["captcha"],
            "robots_txt_detected": anti_bot_signs["robots.txt"],
            "csrf_token_detected": anti_bot_signs["csrf_token"],
            "cloudflare_detected": anti_bot_signs["cloudflare"],
        }

    except requests.exceptions.RequestException as e:
        print(f"Error accessing URL {url}: {e}")
        return {
            "url": url,
            "status_code": "Error",
            "error_message": str(e),
            "response_time": None,
            "content_length": None,
            "content_type": None,
            "captcha_detected": None,
            "robots_txt_detected": None,
            "csrf_token_detected": None,
            "cloudflare_detected": None,
        }


def process_urls(url_list):
    results = []
    for url in url_list:
        result = check_url(url)
        results.append(result)
    return results


def save_to_excel(data, filename):
    df = pd.DataFrame(data)
    df.to_excel(filename, index=False, engine='openpyxl')
    print(f"Results saved to {filename}")


# Example usage
if __name__ == "__main__":

    urls_to_check = [
        'https://www.sbp.org.pk/circulars/index.asp',
        'https://www.secp.gov.pk/media-center/press-releases/',
        'https://www.nab.gov.pk/press/press_release2.asp?curpage=2',
        'https://www.superseguros.gob.pa/sancion/companias-de-seguros/',
        'https://www.gob.pe/institucion/oefa/buscador?term=Sanction&institucion=oefa&topic_id=&contenido=noticias&sort_by=none',
        'https://www.gob.pe/689-relacion-de-proveedores-sancionados-para-contratar-con-elestado',
        'https://www.insurance.gov.ph/notice-to-the-public/',
        'http://www.amlc.gov.ph/advisories/amlc-advisory',
        'https://www.tcontas.pt/pt-pt/ProdutosTC/Decisoes/Pages/Decisoes-do-Tribunal-de-Contas.aspx',
        'https://www.politiaromana.ro/en/most-wanted',
        'http://www.onjn.gov.ro/home/lista-neagra',
        'https://integritate.eu/comunicate-de-presa/',
        'https://www.umucyo.gov.rw/um/ubl/moveUmUblBlacLstComListPubBlacklisted.do',
        'https://www.fsrc.kn/warnings',
        'https://fsaseychelles.sc/media-corner/regulatory-updates',
        'https://www.mom.gov.sg/employment-practices/employers-convicted-under-employment-act#/',
        'https://www.mas.gov.sg/investor-alert-list',
        'https://www.police.gov.sg/Media-Room/News/',
        'https://www.cccs.gov.sg/cases-and-commitments/public-register/abuse-of-dominance',
        'https://nbs.sk/en/financial-market-supervision-practical-info/warnings/warning-list-of-non-authorized-business-activities-of-entities/',
        'https://www.antimon.gov.sk/news/?csrt=12240206745515518130',
        'https://www.policija.si/eng/wanted-persons?view=tiraliceseznam',
        'https://www.bde.es/wbe/en/entidades-profesionales/supervisadas/sanciones-impuestas-banco-espana/',
        'https://www.poderjudicial.es/cgpj/es/Poder-Judicial/Audiencia-Nacional/Noticias-Judiciales/',

    ]
    results = process_urls(urls_to_check)

    # Save results to an Excel file
    output_file = "url_check_results.xlsx"
    save_to_excel(results, output_file)
