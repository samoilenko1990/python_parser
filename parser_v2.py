# -*- coding: utf-8 -*-
import configparser
import logging
import os
import queue
import sys
import shutil
import threading
from random import choice
from time import sleep

import pandas as pd
import requests
from bs4 import BeautifulSoup
from proxyscrape import create_collector
from requests.exceptions import Timeout, ConnectionError, ProxyError

BASE_URL = 'https://www.castorama.ru/'
headers = {
    'authority': 'www.castorama.ru',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'Cookie': 'reload_shop=1; t2s-analytics=11e17cf7-de50-49d2-82ad-14d2794fe406; t2s-p=11e17cf7-de50-49d2-82ad-14d2794fe406; _gcl_au=1.1.1759994396.1620741801; top100_id=t1.7346688.1285292124.1620741801615; _ga=GA1.2.1609859231.1620741802; _gid=GA1.2.777526030.1620741802; _userGUID=0:kok3tltz:f1~xP8j~9HmyccdYGnPcsDfdVSq_fp8n; _ym_uid=1617110828366765234; _ym_d=1620741802; scarab.visitor=%223AC8E457902F71DE%22; _fbp=fb.1.1620741802371.1188464638; c2d_widget_id={%22dcd8bdb3f71cf8de7067b440756b9af9%22:%22[chat]%20030cd86d890f1cd63eba%22}; castorama_nearest_shop=blocked; FSMUID=609a9005c05e0368559533; scarab.profile=%22538112%7C1620742445%22; frontend=mhvge0e8du37nmee8431hi0o5v; abtest=%7B%2216%22%3A0%2C%2219%22%3A1%7D; COMPARE=8923%2C112244; CACHED_FRONT_FORM_KEY=fOgrMqHQCZRgtQxV; _gcl_aw=GCL.1620984053.CjwKCAjwv_iEBhASEiwARoemvK58-IQMwc2j4Oy0iAwvs6pLCC5s9AsUhuwaEsMMy7At_k4KsbRRlBoCZ18QAvD_BwE; _gac_UA-4323892-3=1.1620984054.CjwKCAjwv_iEBhASEiwARoemvK58-IQMwc2j4Oy0iAwvs6pLCC5s9AsUhuwaEsMMy7At_k4KsbRRlBoCZ18QAvD_BwE; _gac_UA-4323892-1=1.1620984056.CjwKCAjwv_iEBhASEiwARoemvK58-IQMwc2j4Oy0iAwvs6pLCC5s9AsUhuwaEsMMy7At_k4KsbRRlBoCZ18QAvD_BwE; frontend_cid=Z2DT6hBW2xFiDjbM; _ym_isad=2; custom_sessionId=1621417856991.2i0mg7vk4; _ym_visorc=w; _dvs=0:kovabtlv:DgZNESf~BKsgyeoWikx~h2nYTs5TPDKX; LAST_CATEGORY=2163; CATEGORY_INFO=%5B%5D; CART=5ac4e627d04676b09a5c6d805696f996; last_visit=1621409243971::1621420043971; dSesn=bc5af13f-f5da-945f-d307-b2ee0e4c4189; _gat_UA-4323892-1=1; castorama_current_shop=48; store=voronezh_ru',
    'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
    'sec-ch-ua-mobile': '?0',
    'dnt': '1',
    'upgrade-insecure-requests': '1',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,ko;q=0.6',
}

catalog_link = []
ALL_LINK_TR_ITEMS = []
ALL_CATEGORY_LINK = []
ALL_VARIABLE_LINK = []
TRADE_ITEMS = []
IMG_URL = []
proxy_list = []


class LoadingTheConfiguration(object):
    def __init__(self, file_name):
        try:
            logging.info('Проверка наличия файла конфигурации в папке {config.conf}')
            self.config = configparser.ConfigParser()
            self.config.read_file(open(file_name))
            logging.info('Файл найден. Загрузка конфигурации скрипта')
            if int(self.config.get("DEFAULT", "download_image")) not in [1, 0]:
                logging.critical('Переменная download_image может принять только значения 0 и 1 (0-выкл, 1-вкл)')
                sys.exit()
            if int(self.config.get("PROXY", "use_free_proxies")) not in [1, 0]:
                logging.critical('Переменная use_free_proxies может принять только значения 0 и 1 (0-выкл, 1-вкл)')
                sys.exit()

            self.count_threads = int(self.config.get("DEFAULT", "count_threads"))
            self.use_free_proxies = True if int(self.config.get("PROXY", "use_free_proxies")) == 1 else False
            self.proxies = True if int(self.config.get("PROXY", "proxies")) == 1 else False
            self.proxiesFile = self.config.get("PROXY", "proxiesFile")
            self.userAgents = self.config.get("DEFAULT", "userAgentsFile")

            self.download_image = True if int(self.config.get("DEFAULT", "download_image")) == 1 else False
        except FileNotFoundError:
            logging.critical("Ошибка при загрузки конфигурации. Файл не найден")
            sys.exit()
        except ValueError as e:
            logging.critical("Ошибка при загрузки конфигурации. Ошибка данных. {%s}" % e)
            sys.exit()
        except TypeError as e:
            logging.critical("Ошибка при загрузки конфигурации. Ошибка типа данных. {%s}" % e)
            sys.exit()
        except configparser.NoSectionError as e:
            logging.critical("Ошибка при загрузки конфигурации. {%s}" % e)
            sys.exit()


class identdict(dict):
    def __missing__(self, key):
        return key


def check(__r):
    if __r.status_code in [200, 404]:
        return True
    else:
        logging.error('Статус %s. Страница не получена %s' % (__r.status_code, __r.url))


def __requests(url, params=None, proxies=None):
    while True:
        try:
            if proxy_list:
                proxies = proxy_list.pop(0)
                if not configuration.use_free_proxies:
                    proxy_list.append(proxies)
            elif configuration.use_free_proxies:
                proxy = collector.get_proxy({'anonymous': True})
                if 'http' not in proxy.type:
                    proxies = {'http': '{}://{}:{}'.format(proxy.type, proxy.host, proxy.port),
                               'https': '{}://{}:{}'.format(proxy.type, proxy.host, proxy.port)}
                else:
                    proxies = {'http': '{}:{}'.format(proxy.host, proxy.port),
                               'https': '{}:{}'.format(proxy.host, proxy.port)}

            with requests.get(url,
                              proxies=proxies,
                              headers={**headers, **{'user-agent': choice(agent_list)}},
                              params=params,
                              timeout=(10, 10)) as r:
                if check(r):
                    if proxies and proxies not in proxy_list:
                        proxy_list.append(proxies)
                    return r
        except (Timeout, ProxyError, ConnectionError):
            pass
        except Exception as e:
            logging.info('Ошибка {} -> {}. Повторяем запрос.'.format(e, url))


def creat_directory(name_d):
    if not os.path.exists('{}{}{}'.format(work_path, sep, name_d)):
        os.makedirs('{}{}{}'.format(work_path, sep, name_d))
    return '{}{}{}'.format(work_path, sep, name_d)


def save_file(_data, f_name):
    __err = 0
    f_name = '{}{}{}'.format(work_path, sep, f_name)
    while True:
        try:
            df = pd.DataFrame(_data)
            logging.info('Сохраняем результат в папку {}'.format(f_name))
            df.to_csv("{}.csv".format(f_name), encoding='utf-8', sep=';', index=False)
            df.to_excel("{}.xlsx".format(f_name), encoding='utf-8', index=False, engine='openpyxl')
            break
        except PermissionError as e:
            logging.error('{}. Операция не может быть завершена. Закройте файл.'.format(e))
            sleep(5)
        except Exception as e:
            logging.error('{}. Операция не может быть завершена. Ошибка записи'.format(e))
            if __err > 20:
                logging.critical('Не удалось сохранить файл.')
                break
            else:
                __err += 1
                sleep(5)


def save_jpeg(item):

    try:

        url = item.get('url')

        folder = item.get('folder')

        f_name = url.split('/')[-1]

        r = requests.get(url, stream=True)

        r.raw.decode_content = True

        with open(f'{work_path}{sep}{folder}{sep}{f_name}', 'wb') as img_file:

            shutil.copyfileobj(r.raw, img_file)

    except Exception as e:

        logging.error('{} | {}'.format(e, item))


def replace_text(val, text):
    if val % 10 == 1 and val % 100 != 11:
        if text in ['изображен']:
            return '%sие' % text
        else:
            return '%sа' % text
    elif 2 <= val % 10 <= 4 and (val % 100 < 10 or val % 100 > 20):
        if text in ['изображен']:
            return '%sия' % text
        else:
            return '%sи' % text
    else:
        if text in ['изображен']:
            return '%sий' % text
        else:
            return '%sок' % text


def is_even(number):
    return number % 10 == 0


def how_much_is_left(count_task, text):
    if is_even(count_task):
        logging.info('Осталось %s %s' % (count_task, replace_text(count_task, text)))


def worker_0():
    while True:
        item = q0.get()
        get_all_pages(item)
        how_much_is_left(q0.unfinished_tasks, 'ссыл')
        q0.task_done()


def worker_1():
    while True:
        item = q1.get()
        find_tr_item(item)
        how_much_is_left(q1.unfinished_tasks, 'ссыл')
        q1.task_done()


def worker_2():
    while True:
        item = q2.get()
        get_product(item)
        how_much_is_left(q2.unfinished_tasks, 'ссыл')
        q2.task_done()


def worker_3():
    while True:
        item = q3.get()
        save_jpeg(item)
        how_much_is_left(q3.unfinished_tasks, 'изображен')
        q3.task_done()


def del_space(text):
    return str(text).rstrip().lstrip()


def del_space_html(text):
    return str(text)


def get_all_pages(url):
    link = url.split('?')[0]
    r = __requests(url=link)
    b_soup = BeautifulSoup(r.content, 'html.parser')
    find_all_trade_items(r.content)
    pagination = b_soup.find('div', {'class': ['pages']})
    if pagination:
        count = [int(x.get('href').split('=')[-1]) for x in pagination.find_all('a')
                 if x.get('href') and x.get('href').split('=')[-1].isdigit()]
        for idx in range(2, max(count) + 1):
            ALL_CATEGORY_LINK.append('%s?p=%s' % (r.url, idx))


def find_tr_item(url):
    r = __requests(url=url)
    find_all_trade_items(r.content)


def find_all_trade_items(html):
    b_soup = BeautifulSoup(html, 'html.parser')
    category_page = b_soup.find('div', {'class': ['category-products']})
    if category_page:
        ALL_LINK_TR_ITEMS.extend([tr_items.find('a').get('href')
                                  for tr_items in category_page.find_all('li', {'class': ['product-card']})
                                  if tr_items.find('a') and tr_items.find('a').get('href')])


def get_product(url):
    item = {'url': url}
    try:
        r = __requests(url=url)
        b_soup = BeautifulSoup(r.content.decode('utf-8', errors='ignore'), 'html.parser')
        variant = b_soup.find('div', {'class': ['swatches']})
        if variant:
            for i in variant.find_all('a'):
                if i.get('href') and i.get('href') not in ALL_LINK_TR_ITEMS:
                    ALL_VARIABLE_LINK.append(i.get('href'))
        title = b_soup.find('title')
        if title:
            item['Title'] = del_space(title.text)
        name = b_soup.find('h1')
        if name:
            item['H1'] = del_space(name.text)

        description = b_soup.find('div', {'class': ['std']})
        if description:
            item['Описание'] = del_space(description.text)

        specifications = b_soup.find('div', {'id': ['specifications']})
        if specifications:
            item['Характеристики'] = del_space(specifications)

        breadcrumb = b_soup.find('div', {'class': ['breadcrumbs']})
        if breadcrumb:
            item['Категория'] = ' | '.join([del_space(i.text) for i in breadcrumb.find_all('a') if i.text][1:])

        availability = b_soup.find('link', {'itemprop': ['availability']})
        if availability:
            item['В наличие'] = '1' if 'InStock' in availability.get('href') else '0'

        price = b_soup.find('span', {'class': ['price']})
        if price:
            item['Цена товара'] = del_space(price.text)

        vendor = b_soup.find('div', {'class': ['product-info-tile _brand']})
        if vendor:
            item['Производитель'] = del_space(vendor.a.text)

        ostatok = b_soup.find('span', {'class': ['shop__count']})
        if ostatok:
            item['Остаток'] = del_space(ostatok.text)

        article = b_soup.find('span', {'itemprop': ['sku']})
        if article:
            item['Артикул'] = del_space(article.text)

        image = b_soup.find('div', {'class': ['product-media__top-slider']})
        if image:
            img = list(set([i.get('data-src') for i in image.find_all('img', {'class': ['top-slide__img']})]))
            item['Изображения'] = ', '.join(img)
            IMG_URL.extend(img)
            if configuration.download_image:
                item['Путь сохранения'] = ', '.join(
                    list(set(['%s%s%s' % (save_path_img, sep, i.get('data-src').split('/')[-1])
                              for i in image.find_all('img', {'class': ['top-slide__img']})])))

        feature = b_soup.find_all('dt')
        if feature:
            for i in feature:
                try:
                    th = del_space(i.text)
                    td = del_space(i.find_next('dd').text)
                    if th and td:
                        item[th] = td
                except:
                    pass

        TRADE_ITEMS.append(item)
    except Exception as e:
        logging.error((e, url))


if __name__ == "__main__":
    sep = '\\'
    file_log = logging.FileHandler('log.log', encoding='utf-8')
    work_path = os.path.join(os.path.dirname(os.path.realpath(__file__)))

    console_out = logging.StreamHandler()

    logging.basicConfig(handlers=(file_log, console_out),
                        format='[%(asctime)s | %(levelname)s]: %(message)s',
                        datefmt='%m.%d.%Y %H:%M:%S',
                        level=logging.INFO)

    logging.info('Запуск скрипта')
    configuration = LoadingTheConfiguration(file_name='config.conf')

    collector = create_collector('collector', ['http', 'https', 'socks4', 'socks5'])
    try:
        logging.info('Получаем файл импорта input.txt')
        with open('{}{}input.txt'.format(work_path, sep), 'r') as input_file:
            for line in input_file.read().splitlines():
                catalog_link.append(line)
        if catalog_link:
            logging.info('Найдено ссылок в файле импорта - %s' % len(catalog_link))
        else:
            logging.error('Ссылки в файле импорта не найдены')
            sys.exit()
    except FileNotFoundError:
        logging.error('Файл input.txt не найден. Выход.')
        sys.exit()
    except Exception as e:
        logging.error(e)

    if configuration.proxies:
        try:
            if configuration.proxiesFile:
                logging.info('Поиск файла с прокси -> %s' % configuration.proxiesFile)
                proxy_list = list(set([i for i in open(configuration.proxiesFile).read().splitlines() if i]))
            else:
                logging.info('Поиск файла с прокси proxy.txt')
                proxy_list = list(set([i for i in open("proxy.txt").read().splitlines() if i]))
            proxy_list = [{'http': f"http://{ip_proxy}", 'https': f"http://{ip_proxy}"}
                          if '@' not in ip_proxy else
                          {'http': 'http://%s' % ip_proxy, 'https': 'http://%s' % ip_proxy}
                          for ip_proxy in proxy_list]
            logging.info('Количество прокси найдено в файле %s' % len(proxy_list))
        except FileNotFoundError:
            if configuration.use_free_proxies:
                logging.info('Файл с прокси не найден. Используем бесплатные прокси')
            else:
                logging.info('Файл с прокси не найден. Парсер не будет использовате прокси при обращении к сайту.')
    elif configuration.use_free_proxies:
        logging.info('Парсер будет использовать бесплатные прокси для обращения к сайту.')
    else:
        logging.info('Парсер не будет использовате прокси при обращении к сайту.')

    try:
        if configuration.userAgents:
            logging.info('Поиск файла с UserAgent -> %s' % configuration.userAgents)
            agent_list = list(set([i for i in open(configuration.userAgents).read().splitlines() if i]))
            logging.info('Количество записей найдено в файле %s' % len(agent_list))
    except FileNotFoundError:
        logging.error('Файл с userAgent не найден. Используем стандартный ua.')
        agent_list = ['Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36']

    q0 = queue.Queue()
    q1 = queue.Queue()
    q2 = queue.Queue()
    q3 = queue.Queue()
    num_work = configuration.count_threads
    logging.info('Создание {} потоков'.format(num_work))
    for i in range(num_work):
        t_0 = threading.Thread(target=worker_0)
        t_0.daemon = True
        t_0.start()
        t_1 = threading.Thread(target=worker_1)
        t_1.daemon = True
        t_1.start()
        t_2 = threading.Thread(target=worker_2)
        t_2.daemon = True
        t_2.start()
        t_3 = threading.Thread(target=worker_3)
        t_3.daemon = True
        t_3.start()

    if configuration.download_image:
        save_path_img = creat_directory(name_d='Images')
    catalog_link = list(set(catalog_link))
    for i in catalog_link:
        q0.put_nowait(i)
    q0.join()

    ALL_CATEGORY_LINK = list(set(ALL_CATEGORY_LINK))
    if ALL_CATEGORY_LINK:
        logging.info('Ссылок пагинации - %s' % len(ALL_CATEGORY_LINK))
        for i in ALL_CATEGORY_LINK:
            q1.put_nowait(i)
        q1.join()

    ALL_LINK_TR_ITEMS = list(set(ALL_LINK_TR_ITEMS))
    if ALL_LINK_TR_ITEMS:
        logging.info('Найдено ссылок на товар - %s' % len(ALL_LINK_TR_ITEMS))
        for i in ALL_LINK_TR_ITEMS:
            q2.put_nowait(i)
        q2.join()

    ALL_VARIABLE_LINK = list(set(ALL_VARIABLE_LINK))
    if ALL_VARIABLE_LINK:
        logging.info('Найдено уникальных ссылок на варианты товара - %s' % len(ALL_VARIABLE_LINK))
        for i in ALL_VARIABLE_LINK:
            q2.put_nowait(i)
        q2.join()

    if TRADE_ITEMS:
        save_file(TRADE_ITEMS, 'result')
        if configuration.download_image:
            IMG_URL = list(set(IMG_URL))
            if IMG_URL:
                logging.info('Приступаем к скачиванию изображений')
                for i in list(set(IMG_URL)):
                    q3.put_nowait({'url': i, 'folder': 'Images'})
                q3.join()
    logging.info('Работа скрипта завершена')
