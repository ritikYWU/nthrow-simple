from bs4 import BeautifulSoup
from nthrow.utils import sha1
from nthrow.source.simple import SimpleSource

'''
extractor.make_a_row method
	make_a_row takes 3 parameters here
	1.) url of the dataset that you put in extractor.set_list_info
	2.) url of a row, 
			always pass it through self.mini_uri method, 
			this replaces https with http and removes www. from urls to reduce duplicate rows
	- hash of urls from 1 & 2 becomes id of the row
	3.) the row data, it's stored in a JSONB column
	
etractor.make_error method
	make_error takes 3 parameters
	1.) _type = HTTP, Exception etc.
	2.) code = 404, 403 etc.
	3.) message = None (Any text message)
'''

class Extractor(SimpleSource):
	def __init__(self, *args, **kwargs):
		super(Extractor, self).__init__(*args, **kwargs)

	def make_url(self, row, _type):
		# args is dict that contains current page cursor, limit and other variables from extractor.query_args, extractor.settings
		args = self.prepare_request_args(row, _type)
		page = args['cursor'] or 1
		return f'https://quotes.toscrape.com/page/{page}/', page

	async def fetch_rows(self, row, _type='to'):
		# row is info about this dataset
		# it is what was returned with extractor.get_list_row method
		# it holds pagination, errors, retry count, next update time etc.
		try:
			url, page = self.make_url(row, _type)
			res = await self.http_get(url)	# wrapper around aiohttp session's get
			
			if res.status == 200:
				content = await res.text()
				soup = BeautifulSoup(content, 'html.parser')
				rows = [
					{
						'uri': f'https://quotes.toscrape.com/#{sha1(e.find(class_="text").get_text())}',
						'author': e.find(class_='author').get_text(),
						'text': e.find(class_='text').get_text(),
						'tags': [t.get_text() for t in e.find_all(class_='tag')],
					}
					for i,e in enumerate(soup.find_all(class_='quote'))
				]
				rows = self.clamp_rows_length(rows)	# slice rows length to limit from extractor.query_args or extractor.settings[remote]
				return {
					'rows': [
						self.make_a_row(row['uri'], self.mini_uri(r['uri'], keep_fragments=True), r) for r in rows
					],
					'state': {
						'pagination': {
							# value for next page, return None when pagination ends
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
