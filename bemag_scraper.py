import re
import requests
import queue
import sqlite3


from threading import Thread


pages_queue = queue.Queue()
data_queue = queue.Queue()


def extract_product_data(htmlpage):
    """ data scraper """
    data = {}
    #product name is found after tag <h1 class="product-title text-red">
    result = re.findall('<h1 class="product-title text-red">.*</h1>', htmlpage)
    product_name = result[0][35:-5]
    data['product_name'] = product_name

    # product price is found after tag <div class="product-price pull-right">
    result = re.findall('<div class="product-price pull-right">.*\n</div>', htmlpage, re.S)
    price = result[0].splitlines()[1]
    price = price.strip()
    product_price = price
    if price.startswith('<del>'):
        index = price.rfind('>')
        index += 1
        product_price = price[index:]
    product_price = product_price.strip()
    data['product_price'] = product_price

    # product category is found after tag <p>Categoria: <strong>
    result = re.findall('<p>Categoria: <strong>.*</strong>', htmlpage)
    product_category = result[0][22:-9]
    data['product_category'] = product_category

    #product url is found after tag <meta property="og:url" content="
    result = re.findall('<meta property="og:url" content=".*/>', htmlpage)
    product_url = result[0][33:-4]
    data['product_url'] = product_url

    return data


def download_page(url):
    """ download html page from url """
    r = requests.get(url, timeout=30)
    return r.text


def download_page_add_to_queue(url):
    """ download html page from url and add it to a global queue """
    r = requests.get(url, timeout=30)
    pages_queue.put_nowait(r.text)


def extract_urls_from_page(htmlpage):
    """ extract urls of products from html page """
    result = re.findall('<a class="media-link" href=.*">', htmlpage)
    urls = []
    for x in result:
        url = x[28:-2]
        urls.append(url)
    return urls


def extract_product_data_get_from_queue():
    """ read a page from the pages queue, extract data and add it to data queue """
    while True:
        try:
            page = pages_queue.get(timeout=30)
            data_queue.put_nowait(extract_product_data(page))
        except queue.Empty:
            break


def print_products_on_page(url):
    """ sequentially download html pages and extract data from them """
    page = download_page(url)
    urls = extract_urls_from_page(page)
    pages = map(download_page, urls)
    result = map(extract_product_data, pages)
    print(list(result))


def parallel_print_products_on_page(url, num_extractor_threads=4):
    """ threaded download and extraction of product data  """
    page = download_page(url)
    urls = extract_urls_from_page(page)

    for addr in urls:
        th = Thread(target=download_page_add_to_queue, args=(addr,))
        th.start()

    for _ in range(num_extractor_threads):
        th = Thread(target=extract_product_data_get_from_queue)
        th.start()

    try:
        while True:
            item = data_queue.get(timeout=15)
            print(item)
    except queue.Empty:
        pass


def connect_to_db(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("PRAGMA synchronous = OFF")
    c.execute("PRAGMA journal_mode = MEMORY")
    return c


def parallel_add_products_to_db(url, cursor, num_extractor_threads=4):
    """ threaded download and extraction of product data into a sqlite db """
    page = download_page(url)
    urls = extract_urls_from_page(page)

    for addr in urls:
        th = Thread(target=download_page_add_to_queue, args=(addr,))
        th.start()

    for _ in range(num_extractor_threads):
        th = Thread(target=extract_product_data_get_from_queue)
        th.start()

    cursor.execute('''CREATE TABLE IF NOT EXISTS products (name text, price text, category text, url text)''')
    
    try:
        while True:
            item = data_queue.get(timeout=15)
            sql = ''' INSERT INTO products(name,price,category,url) VALUES(?,?,?,?) '''
            values = (item['product_name'], item['product_price'], item['product_category'], item['product_url'])
            cursor.execute(sql, values)
    except queue.Empty:
        cursor.connection.commit()
        cursor.connection.close()


if __name__ == '__main__':
    #print_products_on_page('https://www.bemag.ro/bauturi/champagne-spumante/')
    #print_products_on_page('https://www.bemag.ro/bauturi/single-malt-scotch-whisky')
    #parallel_print_products_on_page('https://www.bemag.ro/bauturi/champagne-spumante/')
    #parallel_add_products_to_db('https://www.bemag.ro/bauturi/champagne-spumante/')
    #parallel_add_products_to_db('https://www.bemag.ro/bauturi/single-malt-scotch-whisky')
    cursor = connect_to_db('bemagproducts.db')
    parallel_add_products_to_db('https://www.bemag.ro/bauturi/vodka', cursor)

