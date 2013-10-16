import urllib2
from xml.dom.minidom import parseString
from sets import Set

#query function
def query(user):
	print '====query user:'+user
	f.write('\nquery user:'+user+'\n')
	groups.add(user)
	try:
		c = urllib2.urlopen('http://api.douban.com/people/'+user+'/contacts?start-index=1&max-results=35')
	except urllib2.HTTPError, e:
		f.write('HTTPError = ' + str(e.code) + '\n')
		return
	except urllib2.URLError, e:
		f.write('URLError = ' + str(e.reason) + '\n')
		return
	except httplib.HTTPException, e:
		f.write('HTTPException\n')
		return
	except Exception:
		import traceback
		f.write('generic exception: ' + traceback.format_exc() + '\n')
		return
	contents=c.read()
	c.close()
	dom = parseString(contents)
	xmlTags = dom.getElementsByTagName('entry')
	for xmlNode in xmlTags:
		xmlTag = xmlNode.getElementsByTagName('db:uid')[0].toxml()
		xmlData = xmlTag.replace('<db:uid>','').replace('</db:uid>','')
		print xmlData
		f.write(xmlData + ',')
	for xmlNode in xmlTags:
		xmlTag = xmlNode.getElementsByTagName('db:uid')[0].toxml()
		xmlData = xmlTag.replace('<db:uid>','').replace('</db:uid>','')
		if xmlData in groups:
			print xmlData + ' already in groups'
			return
		else:
			query(xmlData)	
#query function
f = open('output', 'w')
groups = Set([''])
query('tisafu')
f.close()
