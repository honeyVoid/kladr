import requests 
from bs4 import BeautifulSoup
import time
import gspread

from gspread import Client, Worksheet, Spreadsheet


cookies = {
    '_ym_uid': '169392952412752345',
    '_ym_d': '1693929524',
    '_ym_isad': '2',
    'cf_chl_2': 'ff7b6b9ac063fd6',
    'cf_chl_rc_m': '1',
    'cf_clearance': '3EMDoCRIsg4sJrLiFT8Ftj63p2ZdR4AKScioEGj.MoI-1694099242-0-1-8507487a.ab5b5061.4f6d5726-160.2.1694099242',
}

headers = {
    'authority': 'kladr-rf.ru',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    'content-type': 'application/x-www-form-urlencoded',
    # 'cookie': '_ym_uid=169392952412752345; _ym_d=1693929524; _ym_isad=2; cf_chl_2=ff7b6b9ac063fd6; cf_chl_rc_m=1; cf_clearance=3EMDoCRIsg4sJrLiFT8Ftj63p2ZdR4AKScioEGj.MoI-1694099242-0-1-8507487a.ab5b5061.4f6d5726-160.2.1694099242',
    'if-modified-since': 'Wed, 30 Aug 2023 16:58:57 GMT',
    'origin': 'https://kladr-rf.ru',
    'referer': 'https://kladr-rf.ru/93/?__cf_chl_tk=jYVCF0b3boHYI55RefMdwkPoIOGFcd.gYV3E6noHayQ-1694099238-0-gaNycGzNDfs',
    'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
}

data = {
    'md': 'd6GZZKIrWPtt9xb5QZb3whSUNIvFdlqhEXZOKa5WPFQ-1694099238-0-AeotkcmsCAf70IeskgAFQJKOCHGk42Z_KmylN9sboWk1atP4B_CuK8FUJJYo2mFk8N30QcoFO8U2PHtqNk-kSu7-ffAb7E6o3LuSfnl_Q9PkDFyCxARuoOOK7XPOCA2a1PdSWALff7_w4BzGKgAbPiZMlEPKYEHPrWMpivVtS9Vjj40FMp7MoLxsQ8BzhyzJdN3nkm3kBOSwQWQVcFPVVhNGXlymcN8l5YW4IcVyyBkoc19flRtpx-KAygrnxmSYwCqHYs1MoCaW1D5ouCyruhu2Yys-uoD0fTWSqzP9YWAJX1eQGFjr_w7VxzZawpxC2vIJ26-2CVdAtLMN5pR--wKYXg2dt4xtQB1wkb25MsRF15VpH16dU4ngSN2Og5gxOzMWpP9NEWVB1SQXkbH4JGyOhXeTQWNa-wkPeiC48Z20cTj3qaPk2LLw34lxIM91lV0Ynxc9hzZJikSwRDLCpuyN4lf6iOXA7W492h3N7M525kFjgBjuPdGx8HYchF9mjOamFSnFFb7RA-Gif5CKMdSV1blqzuq7cX39tGNKqTJUKgjs7ZDK7_DUr7QEKneQZvGBdnKuGuagoh1-m3Ew4v1gMREbL7K-UHjRfmPcZzdZBrxDgHidBo5jitXFN97wzeBDaft_HxSHXbWperwwqnb2iPZKonVAttzOk7HNGvaOBGLUCzsg4Nq_fFexuc8WTG2jx2gXE4xNpDd3oL6NMHH9sF2x_YOgVzTpwxexW1vAt3A090-kUMTU5TmGkf_AQSQLYN0j8cEwDjSsEb5rs3frfm3vlMuexBcFM8EuqYdrY4fLSleLaXPQfT-mhnwxM9tEWhXr_KaNjO4VrUe-5i-osyxy40knOgTno8-bmry-Rxg_phoSKfiEaQr5A8ZvIWE1XKm9NTEPDvrWyYsEXrxDbRb4lL26Jhz5kOU5g8s5fNSfrubHmW0wZtHRJvQh_AjRXujS2ykXA_1MAZXXgaN2a00AGtcT-iTvTED3yL0Pz9xxj2wyFibxZJaM0viFsHmAz5r1HI172QrMKdYOt_0nNjQYBy7pOqKfiQFHims2AJz4-lYKKNiXq7g9OTQN0oNCVNp4a_laQ0FH7L4JE8oTNrydQr6N_4BOMtMABRcijdOtDtdILMHRRTNHQlrHvixn1Ps2Sm2tGyTCRgmG1gELtPHgNvQuzo9hEIFzjDIYiX1Yl1BNJJKVzDAvTTsTgFrhLqC-KpdGxL2poRzkxpxH8m_8WG3WhE-QvdeGRw5F8MH5NWhavhl2EH3FFPICI6cAc_dZ-5d-ZYg46GZA0siFj0lnLuB5I3QPJ_8s1lRmOaNHOGv3mGPpJNsuYnz0H7jtCA0wXqIS_QfxV7YWDQdC74_2Zyp9W_fboj9dP6VYoVNoWZYw3aEZh9WIelFkORguOkJdZCmLq2ZFj7YL8MvkDbX7J69lhKgyp10L6OknpXdeYhgzpvaeMu2auRiw1D_RNcTnuV_jAc1Q9NnCA6jGFut5x62xqWqztVOsmT1BfpkLPCg3p8KpT59eIm7ODvPVj2yDeIXCR7fGl7KPSFHt8T4VfVatW7TOcKOuIDikZZIg1F9FK-t8b7EhoP6MlQCTQxYB15ExKzPKtA71h1P-1EmTdBR4_hB2Sqpl0okyQBGOrXvIlXauDkxejeDL-AGC060Hsx3rpf0QGXguPhXh2vOpupz8Jq2Y7XKEorRsa12QLVZliWcXCl2gbF4NeWCN49OUqGKTOr0rDW1omfTr3N0YbOsiIvPqr5wRablSWGUUE_C8CoU3aN6CoVraX7WkMrFa3LM7uuDDdtK_MEric1zOehotJflJmKcscHk6pZOxYvJhtQIP_d7cAmT0XzNa0TdDqJefSt7dC9-0RgSLEgpht2h1zhkiMHen2xToQgf3tleQzfXCFk9jjh91eCCAZ7bRcalBlP_zRf40UwH6BtjtXEGQBfMLAtVImIlHOtBeXlitaj6-PvE5bGR31hGTveUL0iNMtF5sK3IbUCTZi0ScXvE0l9_ui996Ctgw1ip10ps3lohFG27EdvtD6OXy_SeeJ9Wx90xAA5G83mfqtUIEPkuiVpYIQ290gLW-FhpmaFDzEC2oVcYhvI_G0w6WJttWH92IdWNUskH2DfSuVULPori_S0PjDKMS5g4On8Cw9qZTtm24qXI1L172d0e7o6Gkj8c6qS8guRl0YYpvk4MPq1FncuJ5IdB-95njmVEhYQqYGs_xDR8CjUiCT62RrNomzXK4Po-BimH-3j8vfKPd1YSJSAIXidHlvmFxarYCD1Z8-X3a0WZ63DQHjOCzXb4MAOV-CoqTaUhuBpxcXQ0u4o0szPhCp6BLcF_ZeIbU8SqtpOL1R_mLW_vwXOMe2658Z_ThwGLbgYy4YklTvL80SFTVC_a4k4rtvdxhh34G3HIqIoK5z0HQbWVDhXDdFPhHH_0G6XLdSVHdAQtKlqB0nySDoa-YrN-A4Jx-8zxPcFVRQ9YEV5ZlGIa2zBCbq_FwBdG4Qj2PTkGlUQMkvTh-lkSpccqFtghbMwq0bOPkcGkWKp1DVlUFLnszzBDq_f8JB2vD_oNdi7dBc8uwKRKXeBP2i9I9Fl5ZA2EVaTUZ0BGrMuCmv1qtGzsgZAAVSN6yzoJ8bs3Sukmao9oUrw2FD6qjoZ-jeB5F6QIx75oerVQ3p0dXS2jV0YuUUZGn_6VW0uJszGGJVV9cvhJ2xAt89U-wMc5rx9ttqTIxmREs1zE7eT9X5MqDcnZIed9W02Ab2KKAxBKCu7P53KpqyklMIbjnnEbcz2QTFzJNAqyCndbtYa_wH-AUhNdLZpWGCCVEDG26Ch0s3g4HhF0Rlv889SWldFjUDRBbIzeEFouuzbMBHOJquBz2JSMQTET6QzYYwaCYP3hfvfYzYVp5dkACjEmhL6c3sWzL4xq_4-xtlmbKOKvkqzgONcSk7B2S4A3-FI0fQIr5ezLym6oqtbfENdGlenEubLzlUA9rfbyTnl_yPMADkZplFYlaivK2Crgffq_GsiuoIPXHKDKRxy49fjuzlsIa8hauvzLJLtKAYTmlWsBX6ZNXOnitTyLkExjp3K-tuk7hXcTifJS8R_t9LlPbAPOuQcVV6vB6oILBKf7cORqoNquKaM8uRyxFGCS-kEIcS6skWJj7ZDGw2Cjf2rOXpeX9LKRZUgw31rOmfulJjIEEqr8oudU9TtuouiGkii1RIgAqnKDU_aqY0JpEPLx4GXqmDwVUGp_MjrQwmzJO-HRMiUW3N1FSEb7k_2cz9LzUfdXYRxN-qiU9OGkbObQ9Yk4a8YoWdEK-zP-PUF6xRmyeO6HQQK9WeAAsAU6Lf6sxsTrobRvDEvZg1SJN38uAPPcj-CW8FE5m4X9bjBV5pZRRnFxgGSC84hD-5sG1X_IYOt6XIOvFRuWHRwxF7aEiKEaNGFHvypshfDkzfRMIzv-5o889GuIS47Q9gc2TvhhCRww_zsAJaTj0yW90ZQXzIRcKz1Y1hNd1GIjieua626G6403rA4wQnQ',
    'sh': '4680c64465b05dc448cc19c33a8585fb',
    'aw': 'QZjijdkTnggg-1-802fdc51dc564d89',
    'cf_ch_cp_return': '6fe21dc4f4d4a84088feacaa2ae0e658|{"managed_clearance":"ni"}',
}


gc: Client = gspread.service_account('cred.json')
sh: Spreadsheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1SMBGnrCvDxCgeiaqH2AL-MeAI5GfpWCRIY3Vnq1H0iw/')
ws: Worksheet = sh.worksheet("95")

hhh = requests.post('https://kladr-rf.ru/95/', cookies=cookies, headers=headers, data=data).text
soup = BeautifulSoup(hhh, 'lxml')
bl = soup.find_all('div', class_='row')

print


def extract_name(place_name: str) -> str:
    location_types = {
        "Село": "с",
        "Поселок городского типа": "пгт",
        "Город": "г",
        "Район": "рн",
        "Поселок": 'п'
    }
    city_name = place_name
    location_type = None

    for location, abbreviation in location_types.items():
        if place_name.endswith(location):
            city_name = place_name[: -len(location)].strip()
            location_type = abbreviation
            break
    return city_name, location_type


def get_(ls: list) -> list:
    href_values = []
    for div in ls:
        uls = div.find_all('ul', class_='col')
        for ul in uls:
            links = ul.find_all('a')
            for link in links:
                href = link.get('href')
                href_values.append(href)
    return href_values


def get_ul_links(ls: list, max_length=16) -> list:
    href_values = []
    for div in ls:
        uls = div.find_all('ul', class_='col')
        for ul in uls:
            links = ul.find_all('a')
            for link in links:
                href = link.get('href')
                if len(href) <= max_length:
                    href_values.append(href)
    return href_values


def sub_citys():
    links_for_links = get_(bl)
    for lin in links_for_links:
        response = requests.post(
            f'https://kladr-rf.ru{lin}',
            cookies=cookies,
            headers=headers,
            data=data
        ).text
        soup = BeautifulSoup(response, 'lxml')
        block = soup.find_all('div', class_='row')
        links = get_ul_links(block)
        print(links)
        rows_to_append = []
        for link in links:
            url = f'https://kladr-rf.ru{link}'
            res = requests.get(
                url=url,
                cookies=cookies,
                headers=headers,
                data=data
            ).text
            soup = BeautifulSoup(res, 'lxml')
            kladr = soup.find_all('span', class_='fw-bold')[1].text
            city_name, loc_type = extract_name(
                soup.find('ol', class_='breadcrumb').find_all('a')[-1].text
            )
            kladr_2 = kladr[:2] + '0' * 11
            rows_to_append.append([city_name, kladr, kladr_2, loc_type])
            time.sleep(0.5)
        print(rows_to_append)
        ws.append_rows(rows_to_append)


sub_citys()
