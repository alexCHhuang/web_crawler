import urllib2
from xml.dom.minidom import parseString

#query function
def query(user):
	print '====query user:'+user
	c = urllib2.urlopen('http://api.douban.com/people/'+user+'/contacts?start-index=1&max-results=35')
	contents=c.read()
	c.close()
	dom = parseString(contents)
	xmlTags = dom.getElementsByTagName('entry')
	for xmlNode in xmlTags:
		xmlTag = xmlNode.getElementsByTagName('db:uid')[0].toxml()
		xmlData = xmlTag.replace('<db:uid>','').replace('</db:uid>','')
		print xmlData
	global globvar
	globvar += 1
	if (globvar ==35):
		return
	else:
		query(xmlData)
#query function
globvar = 0
query('DrityAFei')
