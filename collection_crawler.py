import urllib2
import socket
import MySQLdb
import time
import xml.etree.ElementTree as ET

def INSERTDB(tablename, collectionId, userId, itemId, status, updated):
	if (tablename == tb_book_collection):
		item_id = 'book_id'
	elif (tablename == tb_music_collection):
		item_id = 'music_id'
	else:
		item_id = 'movie_id'

	cursor.execute('SELECT COUNT(*) FROM %s where collection_id="%s"' %(tablename, collectionId))
	result = cursor.fetchone()
	# if this user already exists in the table, just return
	if(result[0] == 0):
		# translate status into integer
		if status == 'read':
			status = 1
		elif status == 'reading':
			status = 2
		elif status == 'listened':
			status = 3
		elif status == 'listening':
			status = 4
		elif status == 'watched':
			status = 5
		elif status == 'watching':
			status = 6
		elif status == 'wish':
			status = 7

		try:
			cursor.execute("INSERT INTO "+tablename+" (collection_id, user_id, "+item_id+", status, updated) VALUES (%s,%s,%s,%s,%s)",
			(collectionId, userId, itemId, status, updated))
			db.commit()
		except:
			import traceback
			print 'generic exception: ' + traceback.format_exc()
			print 'insert DB fail'
			db.rollback()

def GetNextUser(index):
	cursor.execute('SELECT name FROM douban_users_check where id="%s"' %(index))
	result = cursor.fetchone()
	next_user = result[0]
	return next_user

def HandleException():
	global counter
	global user
	global startIndex
	global retry_counter
	if (retry_counter < 5):
		print "Retry: " + str(retry_counter)
		f.write("\n Retry: " + str(retry_counter))
		time.sleep(60)
		retry_counter += 1
	else:
		retry_counter = 0
		counter += 1
		user = GetNextUser(counter)
		f.write('\n Get next user: '+user)
		startIndex = 1

retry_counter = 0
counter = 911
Upperbound = 40000
tb_users = 'users'
tb_book_collection = 'book_collection'
tb_music_collection = 'music_collection'
tb_movie_collection = 'movie_collection'
db = MySQLdb.connect(host="localhost", user="db_user", passwd="db_pass",db="test")
cursor = db.cursor()
startIndex = 1
logname = 'collection_crawler.log'
f = open(logname,'w')
user = 'cure7';
	
while True:
	if (counter > Upperbound):
		break
	print 'index=:' + str(startIndex)
	url = 'http://api.douban.com/people/'+user+'/collection?start-index='+str(startIndex)+'&max-results=50'
	f.write("\n Obtaining user's collection: "+user)
	try:
		c = urllib2.urlopen(url, timeout = 60)
		contents=c.read()
	except socket.timeout, e:
		f.write('\n[ERROR]: socket timeout')
		HandleException()
		continue
	except urllib2.HTTPError, e:
		f.write("\n[ERROR]: urllib2.HTTPError: "+str(e.reason))
		HandleException()
		continue
	except urllib2.URLError, e:
		f.write('\n[ERROR]: urllib2.URLError: '+str(e.reason))
		HandleException()
		continue

	c.close()
	retry_counter = 0
	try:
		root = ET.fromstring(contents)
	except xml.parsers.expat.ExpatError, ex:
		f.write('\n[ERROR]: XML parse error: '+str(ex.reason))
		HandleException()
		continue
	if (startIndex == 1):
		# find user id
		elements = root.find('{http://www.w3.org/2005/Atom}author')
		element = elements.find('{http://www.w3.org/2005/Atom}uri')
		user_uri = element.text
		user_id = user_uri.rpartition('/')[2]
		
		# insert data into users table
		cursor.execute('SELECT COUNT(*) FROM users where id="%s"' %(user_id))
		result = cursor.fetchone()
		# if this user already exists in the table, just return
		if(result[0] == 0):
			try:
				cursor.execute("INSERT INTO users (id, name) VALUES (%s,%s)",(user_id, user))
				db.commit()
			except:
				import traceback
				print 'generic exception: ' + traceback.format_exc()
				print 'insert DB fail'
				db.rollback()
	# END IF
	entries = root.findall('{http://www.w3.org/2005/Atom}entry')

	for entry in entries:
		# find collection id
		element = entry.find('{http://www.w3.org/2005/Atom}id')
		collection_uri = element.text
		collection_id = collection_uri.rpartition('/')[2]
		
		# find item id
		detail = entry.find('{http://www.douban.com/xmlns/}subject')
		element = detail.find('{http://www.w3.org/2005/Atom}id')
		item_uri = element.text
		item_id = item_uri.rpartition('/')[2]
		
		# find item updated
		element = entry.find('{http://www.w3.org/2005/Atom}updated')
		item_updated = element.text
		updated = item_updated.rpartition('+')[0]

		#find item status
		element = entry.find('{http://www.douban.com/xmlns/}status')
		status = element.text
		
		# find category
		element = detail.find('{http://www.w3.org/2005/Atom}category')
		category_uri = element.get('term')
		category = category_uri.rpartition('#')[2]

		tb_collection = ''
		if (category == 'book'):
			tb_collection = tb_book_collection
		elif (category == 'music'):
			tb_collection = tb_music_collection
		else:	# movie collection
			tb_collection = tb_movie_collection
	
		# insert data into collection table
		INSERTDB(tb_collection, collection_id, user_id, item_id, status, updated)


	if (len(entries) > 0):
		startIndex += 50
	else:
		f.write("\n[Completed]: user's collection is complete\n")
		counter += 1
		user = GetNextUser(counter)
		f.write('\n Get next user: '+user)
		startIndex = 1

db.close()
