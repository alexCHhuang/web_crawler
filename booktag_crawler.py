import urllib2
import socket
import MySQLdb
import time
import xml.etree.ElementTree as ET



tb_booktag = 'book_tag'
db = MySQLdb.connect(host="localhost", user="db_user", passwd="db_pass",db="test", charset="utf8")
cursor = db.cursor()

def GetNextBookId():
	cursor.execute("SELECT book_id from book_collection WHERE book_id NOT IN (SELECT book_id FROM book_tag) limit 1")
	result = cursor.fetchone()
	if(result[0] == 0):
		return ''
	else:
		return str(result[0])

while True:
	book_id = GetNextBookId()
	if (book_id == ''):
		break
	print "Get book id:"+book_id+" tag"
	url = 'http://api.douban.com/book/subject/'+book_id
	try:
		c = urllib2.urlopen(url, timeout = 60)
		contents=c.read()
	except socket.timeout, e:
		print "Time out"
		time.sleep(60)
		continue
	except urllib2.HTTPError, e:
		print "Time out"
		time.sleep(60)
		continue
	except urllib2.URLError, e:
		print "Time out"
		time.sleep(60)
		continue
	c.close()
	root = ET.fromstring(contents)
	elements = root.findall('{http://www.douban.com/xmlns/}tag')
	if (len(elements) == 0):
		cursor.execute("INSERT INTO book_tag(book_id, tag) VALUES (%s,NULL)",(book_id))
		db.commit()
		
	for tag in elements:
		element = tag.get('name')
		print element
		# Insert into DB
		try:
			cursor.execute("INSERT INTO book_tag(book_id, tag) VALUES (%s,%s)",(book_id,element))
			db.commit()
		except:
			import traceback
			print 'generic exception: ' + traceback.format_exc()
			print 'insert DB fail'
			db.rollback()
