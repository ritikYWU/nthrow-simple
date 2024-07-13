import os
import asyncio

from datetime import datetime, timedelta
from nthrow.utils import create_db_connection, create_store, utcnow
from nthrow.utils import uri_clean, uri_row_count

from extractor import Extractor

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
	l.set_list_info('https://quotes.toscrape.com/')
	
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
			
			# collect_rows calls fetch_rows on your extractor class and puts the returned rows in postgres table
			r = await l.collect_rows(l.get_list_row())
			row = l.get_list_row()
			
			assert type(row['next_update_at']) == datetime
			assert row['next_update_at'] <= utcnow()
			to = row['state']['pagination']['to']
			assert row['state']['pagination']['to']
			assert not row['state']['pagination']['from']
			
			row_count = uri_row_count(l.uri, conn, table, partial=False)
			assert row_count >= 10
			
			# if the pagination has next page info, should_run_again() will return true so you can run the extractor again
			assert l.should_run_again() == True
			
			l.session = None #simulate error
			
			r = await l.collect_rows(row)
			row = l.get_list_row()

			assert type(row['next_update_at']) == datetime
			assert row['next_update_at'] <= utcnow()
			assert row['state']['pagination']['to'] == to
			assert 'error' in row['state']
			assert row['state']['error']['primary']['times'] == 1
			assert uri_row_count(l.uri, conn, table, partial=False) == row_count
					
			l.session = session		
			l._reset_run_times()	# this method resets class property used by should_run_again()
			
			r = await l.collect_rows(row)
			row = l.get_list_row()
			
			assert type(row['next_update_at']) == datetime
			assert row['next_update_at'] <= utcnow()
			assert row['state']['pagination']['to']
			assert row['state']['pagination']['to'] != to
			assert not row['state']['pagination']['from']
			assert set(row['state'].keys()) == {'pagination', 'last_run'}
			assert uri_row_count(l.uri, conn, table, partial=False) > row_count
			assert l.should_run_again() == True			
			
	asyncio.run(call())


if __name__ == '__main__':
	pass
	test_simple_extractor()
