import re
import requests

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

def getpage(url):
    """ get html page from url """
    r = requests.get(url)
    return r.text

def extract_product_url(htmlpage):
    """ extract product urls """
    result = re.findall('<a class="media-link" href=.*">', htmlpage)
    urls = []
    for x in result:
        url = x[28:-2]
        urls.append(url)
    return urls

def print_products_on_page(url):
    page = getpage(url)
    product_list = extract_product_url(page)
    pages = map(getpage, product_list)
    result = map(extract_product_data, pages)
    print(list(result))

print_products_on_page('https://www.bemag.ro/bauturi/champagne-spumante/')
print_products_on_page('https://www.bemag.ro/bauturi/single-malt-scotch-whisky')

