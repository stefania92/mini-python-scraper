import unittest
import bemag_scraper
import sqlite3


class BemagScraperTestCase(unittest.TestCase):

    def test_download_page(self):
        result = bemag_scraper.download_page('https://www.bemag.ro/bauturi/vodka')
        self.assertIn('https://www.bemag.ro/produse/vodka/absolut-100-1l', result)

    def test_extract_urls_from_page(self):
        page = bemag_scraper.download_page('https://www.bemag.ro/bauturi/champagne-spumante/')
        result = bemag_scraper.extract_urls_from_page(page)
        self.assertIn('https://www.bemag.ro/produse/champagne-spumante/alira-champagne-075l', result)

    def test_parallel_add_products_to_db(self):
        cursor = bemag_scraper.connect_to_db('bemagproducts.db')
        bemag_scraper.parallel_add_products_to_db('https://www.bemag.ro/bauturi/gin', cursor)
        conn = sqlite3.connect('bemagproducts.db')
        c = conn.cursor()
        c.execute('SELECT * FROM products WHERE category="Gin"')
        result = c.fetchone()
        self.assertIsNotNone(result)
        c.close()
        conn.close()


if __name__ == '__main__':
    unittest.main()