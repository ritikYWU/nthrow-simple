import os
import asyncio

from datetime import datetime, timedelta
from nthrow.utils import create_db_connection, create_store, utcnow
from nthrow.utils import uri_clean, uri_row_count

from republica.extractor import Extractor

# table name and postgres credentials
table = 'nthrows'
creds = {
	'user': os.environ['DB_USER'],
	'password': os.environ['DB_PASSWORD'],
	'database': os.environ['DB'],
	'host': os.environ['DB_HOST'],
	'port': os.environ['DB_PORT']
}

conn = create_db_connection(**creds)
create_store(conn, table)	# creates table


def test_simple_extractor():
	l = Extractor(conn, table)
	
	# url of your dataset, this effectively becomes id of this dataset
	# use l.get_list_row() to return this record from database later
	l.set_list_info('https://myrepublica.nagariknetwork.com/')
	
	# truncate table to remove previous run's rows
	uri_clean(l.uri, conn, table)

	'''
		l.settings = {
			'remote': {
				# how often to refresh this dataset (in miutes)
				# you can leave it None but this extractor will only run once (will still run until pagination ends)
				'refresh_interval': None,
				
				# number if remote url accepts a limit parameter
				'limit': None
			}
		}
	'''
	
	async def call():
		async with l.create_session() as session:
			l.session = session
			l.query_args['limit'] = 3
			
			r = await l.collect_rows(l.get_list_row())
			assert uri_row_count(l.uri, conn, table, partial=False) == 0
			assert uri_row_count(l.uri, conn, table, partial=True)
			assert l.should_run_again() == True

			# l.session = None #simulate error
			
			l._reset_run_times()
			
			# expand_rows will call expand_partial_rows in your extractor class with a list of partial rows as argument
			# and store returned results in postgres table
			r = await l.expand_rows()
			assert uri_row_count(l.uri, conn, table, partial=False) == 0
			assert uri_row_count(l.uri, conn, table, partial=True) == 3
			assert l.should_run_again() == True
			
			l.session = session
			l._reset_run_times()
			r = await l.expand_rows()
			assert uri_row_count(l.uri, conn, table, partial=False) == 3
			assert uri_row_count(l.uri, conn, table, partial=True) == 0
			assert l.should_run_again() == False
		
			
	asyncio.run(call())


if __name__ == '__main__':
	pass
	test_simple_extractor()
