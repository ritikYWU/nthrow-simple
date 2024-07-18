import asyncio

from bs4 import BeautifulSoup
from nthrow.utils import sha1
from nthrow.source.simple import SimpleSource


class Extractor(SimpleSource):
	def __init__(self, *args, **kwargs):
		super(Extractor, self).__init__(*args, **kwargs)

	def make_url(self, row, _type):
		# args is dict that contains current page cursor, limit and other variables from extractor.query_args, extractor.settings
		args = self.prepare_request_args(row, _type)
		page = args['cursor'] or 1
		keyword = 'cash'
		return f'https://myrepublica.nagariknetwork.com/news/ajax/query?key={keyword}&page={page}/', page

	async def fetch_rows(self, row, _type='to'):
		try:
			url, page = self.make_url(row, _type)
			res = await self.http_get(url, headers={'x-requested-with': 'XMLHttpRequest'})

			if res.status == 200:
				content = await res.json()
				soup = BeautifulSoup(content['template'], 'html.parser')
				articles = soup.find_all('li', class_='listedResult')
				rows = [
					self.parse_content(article)
					for article in articles
				]
				rows = self.clamp_rows_length(rows)
				return {
					'rows': [
						self.make_a_row(row['uri'], self.mini_uri(r['uri'], keep_fragments=True), r, partial=True) for r in rows
					],
					'state': {
						'pagination': {
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
		
	@classmethod
	def parse_content(cls, article):
		base_url = 'https://myrepublica.nagariknetwork.com'
		author_categories = article.find('span', class_='smallTag text-muted').text.strip().replace('\n', '')
		data = {
                'uri': base_url + article.find('a')['href'],
                'author': author_categories.split('By:')[1].split('|')[0].strip(),
                'title': article.find('h4').text.replace('\n', '').strip(),
                'text': article.find('p', class_='text-default').text,
            }
		return data
	
	async def expand_partial_rows(self, rows):
		tasks = []
		for r in rows:
			task = asyncio.create_task(self.http_get(r['data']['uri']))
			task.row = r
			tasks.append(task)
			
		res = []
		done, pending = await asyncio.wait(tasks)
				
		for task in done:
			res.append(task.row)
			if task.exception():
				exc = task.exception()
				self.logger.exception(exc)
				self.merge_error(task.row, self.make_error(type(exc), str(exc), task.row['data']['url']))
				continue
			resp = task.result()
			if resp.status == 200:
				content = await resp.text()
				soup = BeautifulSoup(content, 'html.parser')
				title = soup.find('div', class_='main-heading').find('h2').text.strip()
				published_date = soup.find('div', class_='headline-time pull-left') \
                    .find('p').text.strip().split('By:')[0].split('On:')[1].strip()
				content_elements = soup.find('div', id='newsContent') \
                    .find_all('p')
				content = ''.join(map(lambda x: x.text, content_elements[:-1]))
				tag_list = soup.find_all('li', class_='list-inline-item')
				for tag in tag_list:
					tag.append(tag.text.replace('\n', ''))
				task.row[3]['title'] = title
				task.row[3]['text'] = content
				task.row[3]['published_date'] = published_date
				task.row[3]['tags'] = tag
				task.row['partial'] = False

			else:
				self.merge_error(task.row, self.make_error('HTTP', resp.status, task.row['data']['url']))
		return res