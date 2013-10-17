import urllib2
import socket
import MySQLdb
import datetime
from xml.dom.minidom import parseString

def INSERTDB(username):
	try:
		cursor.execute("INSERT INTO douban_users (name, contact) VALUES (%s,-1)", (username))
		db.commit()
	except:
		import traceback
		print 'generic exception: ' + traceback.format_exc()
		print 'insert DB fail'
		db.rollback()


def DELETEENTRY(username):
	cursor.execute("""DELETE FROM douban_users WHERE name=%s""",username)
	print 'user: ' + username + ' has deleted from database'

def FINDNEXTUSR():
	cursor.execute("SELECT name FROM douban_users where contact=-1 limit 1")
	result = cursor.fetchone()
	if result != None:
		nextuser = result[0]
		print 'next user is found: ' + nextuser
	else:
		nextuser = ''
	startIndex = 1
	return nextuser
	
now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
filename = 'output/output_'+now
f = open(filename,'w')
db = MySQLdb.connect(host="localhost", user="db_user", passwd="db_pass",db="test")
cursor = db.cursor()
friend = 0
startIndex = 1
user = 'ygy8245'
# set global default timeout is 20 seconds
socket.setdefaulttimeout(20)
INSERTDB(user)
while user != '':
	print '[query user]:'+user+ ' start index='+str(startIndex)
	f.write('\n[query user]:'+user+ ' start index='+str(startIndex)+'\n')
	try:
		url = 'http://api.douban.com/people/'+user+'/contacts?start-index='+str(startIndex)+'&max-results=50'
		c = urllib2.urlopen(url)
	except socket.timeout, e:
		print 'socket timeout'
		f.write("There was an error = %r" % e)
		DELETEENTRY(user)
		user = FINDNEXTUSR()
		continue
	except urllib2.HTTPError, e:
		f.write('HTTPError = ' + str(e.code) + '\n')
		DELETEENTRY(user)
		user = FINDNEXTUSR()
		continue
	except urllib2.URLError, e:
		f.write('URLError = ' + str(e.reason) + '\n')
		DELETEENTRY(user)
		user = FINDNEXTUSR()
		continue
	except httplib.HTTPException, e:
		f.write('HTTPException\n')
		DELETEENTRY(user)
		user = FINDNEXTUSR()
		continue
	except Exception:
		import traceback
		f.write('generic exception: ' + traceback.format_exc() + '\n')
		DELETEENTRY(user)
		user = FINDNEXTUSR()
		continue
	contents=c.read()
	c.close()
	dom = parseString(contents)
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
		# if this is new user, insert it into DB
		if(result[0] == 0):
			INSERTDB(xmlData)
	if entryLength == 0:
		print 'search for ' + user + ' is complete, find next user...\n'
		f.write('\nsearch for '+user+ ' is complete, find nex user...\n')
		startIndex = 1
		# UPDATEDB
		cursor.execute('UPDATE douban_users	SET contact = "%d" WHERE name = "%s"' %(friend, user))		
		user = FINDNEXTUSR()
		friend = 0
	else:
		startIndex += 50
f.close()
db.close()
