from bs4 import BeautifulSoup
from nthrow.utils import sha1
from nthrow.source.simple import SimpleSource


class Extractor(SimpleSource):
    def __init__(self, *args, **kwargs):
        super(Extractor, self).__init__(*args, **kwargs)

    def clean_text(self, text):
        cleaned_text = text.strip().replace("\n", "")
        return cleaned_text
    
    def make_url(self, row, _type):
        args = self.prepare_request_args(row, _type)
        page = args['cursor'] or 1	
        return f'https://books.toscrape.com/catalogue/page-{page}.html', page

    async def fetch_rows(self, row, _type="to"):
        try:
            url, page = self.make_url(row, _type)
            res = await self.http_get(url)

            if res.status == 200:
                content = await res.text()
                soup = BeautifulSoup(content, "html.parser")
                books = soup.find_all('li', class_="col-xs-6 col-sm-4 col-md-3 col-lg-3")
                rows = [
                    {
                        "uri": f"https://books.toscrape.com/{sha1(book.find('h3').text)}",
                        "name": self.clean_text(book.find("h3").text),
                        "price": self.clean_text(book.find("p", class_="price_color").text),
                        "stock": self.clean_text(book.find("p", class_="instock availability").text)
                    }
                    for book in books
                ]
                # print(len(table))
                return {
                    "rows": [
                        self.make_a_row(row["uri"], self.mini_uri(r["uri"], keep_fragments=True), r) for r in rows
                    ],
                    "state": {
                        "pagination":{
                            _type: page+1
                        }
                    }
                }
            else:
                self.logger.error('Non-200 HTTP response: %s : %s' % (res.status, url))
                return self.make_error('HTTP', res.status, url)

        except Exception as e:
            self.logger.exception(e)
            return self.make_error('Exception', type(e), str(e))
