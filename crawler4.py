import urllib2
import socket
import MySQLdb
import datetime
import sys
import xml.parsers.expat
from xml.dom.minidom import parseString

def INSERTDB(tablename, username, contact):
	cursor.execute('SELECT COUNT(*) FROM %s where name="%s"' %(tablename, username))
	result = cursor.fetchone()
	# if this user already exists in the table, just return
	if(result[0] == 0):
		try:
			cursor.execute("INSERT INTO "+tablename+" (name, contact) VALUES (%s,%s)", (username, contact))
			db.commit()
		except:
			import traceback
			print 'generic exception: ' + traceback.format_exc()
			print 'insert DB fail'
			db.rollback()

def DELETEENTRY(tablename, username):
	cursor.execute("DELETE FROM "+tablename+" WHERE name=%s",(username))
	print 'user: ' + username + ' has deleted from ' + tablename

def FINDNEXTUSR():
	global startIndex
	sqlcommand = "SELECT name FROM douban_users AS r1 JOIN (SELECT (RAND() * (SELECT MAX(id) FROM douban_users)) AS id) AS r2 WHERE r1.id >= r2.id ORDER BY r1.id ASC LIMIT 1;"

	cursor.execute(sqlcommand)
	result = cursor.fetchone()
	if result != None:
		nextuser = result[0]
		print 'next user is found: ' + nextuser
	else:
		nextuser = ''
	startIndex = 1
	return nextuser

db_name_initial = 'douban_users'
db_name_check = 'douban_users_check'
now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
filename = 'output/output_'+now
f = open(filename,'w')
db = MySQLdb.connect(host="localhost", user="db_user", passwd="db_pass",db="test")
cursor = db.cursor()
friend = 0
startIndex = 1
user = 'oocc'
# set global default timeout is 20 seconds
socket.setdefaulttimeout(20)
INSERTDB(db_name_initial, user, -1)
while user != '':
	if (startIndex == 1):
		#check whether this user is under checking by other process
		cursor.execute("SELECT undercheck FROM "+db_name_initial+" where name=%s",user)
		result = cursor.fetchone()
		if(result[0] == 1):
			user = FINDNEXTUSR()
			continue
		# mark this user is under checking
		cursor.execute("UPDATE "+db_name_initial+" SET undercheck=1 where name=%s", user)
		db.commit()
		print '[START query user: ' + user + ']'
		f.write('\n[START query user: ' + user + ']')
	else:	
		print '[query user: ' + user + ' friend list from='+str(startIndex) + ']'
		f.write('\n[query user: ' + user + ' friend list from='+str(startIndex) + ']')
	
	url = 'http://api.douban.com/people/'+user+'/contacts?start-index='+str(startIndex)+'&max-results=50'
	
	try:
		c = urllib2.urlopen(url)
		contents=c.read()
		c.close()
	except socket.timeout, e:
		print 'socket timeout'
		f.write("\nThere was an error = %r" % e)
		# remark this user
		cursor.execute("UPDATE "+db_name_initial+" SET undercheck=0 where name=%s",(user))
		db.commit()
		user = FINDNEXTUSR()
		continue
	except urllib2.HTTPError, e:
		f.write('\nHTTPError = ' + str(e.code) + '\n')
		cursor.execute("UPDATE "+db_name_initial+" SET undercheck=0 where name=%s",(user))
		db.commit()
		user = FINDNEXTUSR()
		continue
	except urllib2.URLError, e:
		f.write('\nURLError = ' + str(e.reason) + '\n')
		cursor.execute("UPDATE "+db_name_initial+" SET undercheck=0 where name=%s",(user))
		db.commit()
		user = FINDNEXTUSR()
		continue
	except Exception:
		import traceback
		f.write('\ngeneric exception: ' + traceback.format_exc() + '\n')
		cursor.execute("UPDATE "+db_name_initial+" SET undercheck=0 where name=%s",(user))
		db.commit()
		user = FINDNEXTUSR()
		continue
	try:
		dom = parseString(contents)
	except xml.parsers.expat.ExpatError, ex:
		f.write('\nURLError = ' + str(ex.code) + '\n')
		cursor.execute("UPDATE "+db_name_initial+" SET undercheck=0 where name=%s",(user))
		db.commit()
		user = FINDNEXTUSR()
		continue
	print ' {'
	f.write(' {')
	xmlTags = dom.getElementsByTagName('entry')
	entryLength = dom.getElementsByTagName('entry').length
	friend += entryLength
	for xmlNode in xmlTags:
		xmlTag = xmlNode.getElementsByTagName('db:uid')[0].toxml()
		xmlData = xmlTag.replace('<db:uid>','').replace('</db:uid>','')
		print xmlData
		f.write(xmlData + ',')
		# check if this user already exists in DB
		cursor.execute('SELECT COUNT(*) FROM douban_users where name="%s"' %(xmlData))
		result = cursor.fetchone()
		cursor.execute('SELECT COUNT(*) FROM douban_users_check where name="%s"' %(xmlData))
		result_check = cursor.fetchone()
		# if this is new user, insert it into DB
		if(result[0] == 0 and result_check[0] == 0):
			INSERTDB(db_name_initial, xmlData, -1)
		# delete the user from douban_users table if this user already exists in douban_users_check table
		if(result[0] > 0 and result_check[0] > 0):
			print 'Delete duplicate user: ' + xmlData
			DELETEENTRY(db_name_initial, xmlData)
			
	print '}'
	f.write('}\n')
	if entryLength == 0:
		print '[COMPLETE query user: ' + user + ']'
		f.write('[COMPLETE query user: ' + user + ']\n')
		print 'search for ' + user + ' is complete, find next user...\n'
		f.write('\nsearch for '+user+ ' is complete, find next user...\n')
		startIndex = 1
		# UPDATEDB
		INSERTDB(db_name_check, user, friend)
		DELETEENTRY(db_name_initial, user)
		user = FINDNEXTUSR()
		friend = 0
	else:
		startIndex += 50
f.close()
db.close()
