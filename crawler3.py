import urllib2
import MySQLdb
from xml.dom.minidom import parseString

def INSERTDB(username, check):	
	try:
		cursor.execute('insert into douban_users values ("%d","%s","%d")' %(counter,username,check))
		db.commit()
	except:
		db.rollback()
def DELETEENTRY(username):
	cursor.execute("""DELETE FROM douban_user WHERE name=%s""",username)

def FINDNEXTUSR():
	cursor.execute("SELECT name FROM douban_users where ischeck=0 limit 1")
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
counter = 1
user = 'tisafu'
INSERTDB(user,1)
while user != '':
	print '====query user:'+user
	f.write('\nquery user:'+user+'\n')
	try:
		c = urllib2.urlopen('http://api.douban.com/people/'+user+'/contacts?start-index=1&max-results=35')
	except urllib2.HTTPError, e:
		f.write('HTTPError = ' + str(e.code) + '\n')
		DELETEENTRY(user)
		uesr = FINDNEXTUSER()
		continue
	except urllib2.URLError, e:
		f.write('URLError = ' + str(e.reason) + '\n')
		DELETEENTRY(user)
		uesr = FINDNEXTUSER()
		continue
	except httplib.HTTPException, e:
		f.write('HTTPException\n')
		DELETEENTRY(user)
		uesr = FINDNEXTUSER()
		continue
	except Exception:
		import traceback
		f.write('generic exception: ' + traceback.format_exc() + '\n')
		DELETEENTRY(user)
		uesr = FINDNEXTUSER()
		continue
# UPDATE DB
	cursor.execute("""
	UPDATE douban_users
	SET ischeck=1
	WHERE name=%s
	""", (user))		

	contents=c.read()
	c.close()
	dom = parseString(contents)
	xmlTags = dom.getElementsByTagName('entry')
	for xmlNode in xmlTags:
		xmlTag = xmlNode.getElementsByTagName('db:uid')[0].toxml()
		xmlData = xmlTag.replace('<db:uid>','').replace('</db:uid>','')
		print xmlData
		f.write(xmlData + ',')
		cursor.execute('SELECT COUNT(*) FROM douban_users where name="%s"' %(xmlData))
		result = cursor.fetchone()
		if(result[0] == 0):
			counter += 1
			INSERTDB(xmlData,0)
	user = FINDNEXTUSR()
f.close()
db.close()
