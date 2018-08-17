from readTable import *
from collections import *
import sys
readMetaData = Table()
metaOut = readMetaData.readColumns('./metadata.txt')
readMetaData.readData('table1.csv', 'table1')
allTables=(readMetaData.readData('table2.csv', 'table2'))
command,temp = sys.argv[1],[]
ansTable,ansRows,FL = readMetaData.runQuery(command)
if FL == 0:
	for i in ansRows:
		print i,
	print
	for i in ansTable:
		for j in ansRows:
			print i[ansRows.index(j)],
		print 
		
