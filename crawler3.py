import urllib2
import MySQLdb
from xml.dom.minidom import parseString

def INSERTDB(username, contact):	
	try:
		cursor.execute('insert into douban_users values ("%d","%s","%d")' %(counter,username,contact))
		db.commit()
	except:
		db.rollback()


def DELETEENTRY(username):
	cursor.execute("""DELETE FROM douban_users WHERE name=%s""",username)

def FINDNEXTUSR():
	cursor.execute("SELECT name FROM douban_users where contact=-1 limit 1")
	result = cursor.fetchone()
	if result != None:
		print result[0]
		nextuser = result[0]
	else:
		nextuser = ''
	return nextuser
	

f = open('output','w')
db = MySQLdb.connect(host="localhost", user="db_user", passwd="db_pass",db="test")
cursor = db.cursor()
initial = -1
friend = 0
counter = 1
startIndex = 1
user = 'tisafu'
INSERTDB(user,initial)
while user != '':
	print '[query user]:'+user+ ' start index='+startIndex
	f.write('\n[query user]:'+user+ ' start index='+startIndex+'\n')
	try:
		url = 'http://api.douban.com/people/'+user+'/contacts?start-index='+str(startIndex)+'&max-results=50'
		c = urllib2.urlopen(url)
	except urllib2.HTTPError, e:
		f.write('HTTPError = ' + str(e.code) + '\n')
		DELETEENTRY(user)
		uesr = FINDNEXTUSR()
		continue
	except urllib2.URLError, e:
		f.write('URLError = ' + str(e.reason) + '\n')
		DELETEENTRY(user)
		uesr = FINDNEXTUSR()
		continue
	except httplib.HTTPException, e:
		f.write('HTTPException\n')
		DELETEENTRY(user)
		uesr = FINDNEXTUSR()
		continue
	except Exception:
		import traceback
		f.write('generic exception: ' + traceback.format_exc() + '\n')
		DELETEENTRY(user)
		uesr = FINDNEXTUSR()
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
			counter += 1
			INSERTDB(xmlData,initial)
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
