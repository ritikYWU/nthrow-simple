from bs4 import BeautifulSoup
from nthrow.utils import sha1
from nthrow.source.simple import SimpleSource


class Extractor(SimpleSource):
    def __init__(self, *args, **kwargs):
        super(Extractor, self).__init__(*args, **kwargs)

    def clean_text(self, text):
        cleaned_text = text.strip().replace("\n", "")
        return cleaned_text

    async def fetch_rows(self, row, _type="to"):
        try:
            url = 'https://www.imdb.com/chart/moviemeter/'
            cookies = {
            }
            headers = {
                'sec-gpc': '1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            }
            res = await self.http_get(url=url, cookies=cookies, headers=headers)

            if res.status == 200:
                content = await res.text()
                soup = BeautifulSoup(content, "html.parser")

                movies = soup.find_all("li", class_="ipc-metadata-list-summary-item sc-10233bc-0 iherUv cli-parent")
                
                rows = [
                    {
                        "uri": f"https://www.imdb.com/chart/moviemeter/{movie.find('h3', class_='ipc-title__text').text}",
                        "title": movie.find("h3", class_="ipc-title__text").text,
                        "release_date": movie.find("span", class_="sc-b189961a-8 kLaxqf cli-title-metadata-item").text
                    }
                    for movie in movies
                ]

                return {
                    'rows': [
						self.make_a_row(row["uri"], self.mini_uri(r["uri"], keep_fragments=True), r) for r in rows
					],
					'state': {
						'pagination': {
							_type: None
						}
					}
                }


            else:
                self.logger.error('Non-200 HTTP response: %s : %s' % (res.status, url))
                return self.make_error('HTTP', res.status, url)

        except Exception as e:
            self.logger.exception(e)
            return self.make_error('Exception', type(e), str(e))
