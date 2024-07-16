import os
import asyncio

from datetime import datetime, timedelta
from nthrow.utils import create_db_connection, create_store, utcnow
from nthrow.utils import uri_clean, uri_row_count

from ranking.extractor import Extractor

table = 'nthrows'
creds = {
	'user': os.environ['DB_USER'],
	'password': os.environ['DB_PASSWORD'],
	'database': os.environ['DB'],
	'host': os.environ['DB_HOST'],
	'port': os.environ['DB_PORT']
}

conn = create_db_connection(**creds)
create_store(conn, table)	

def test_simple_extractor():
	l = Extractor(conn, table)
	
	l.set_list_info('https://books.toscrape.com/')
	
	uri_clean(l.uri, conn, table)

	async def call():
		async with l.create_session() as session:
			l.session = session
			r = await l.collect_rows(l.get_list_row())
			print(r)
			
	asyncio.run(call())


if __name__ == '__main__':
	pass
	test_simple_extractor()
