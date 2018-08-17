from collections import *
from itertools import permutations
import sys,re,csv
class Table:
	def __init__(self):
		self.tables = defaultdict(dict)
		self.table_flag=0
		self.table_name=""
		self.rowOrder = defaultdict(list)
		self.aggregate = ['max', 'min', 'sum', 'avg','dis']
		self.rowTables = defaultdict(list)
		self.starRow = []
		self.starAns = []
		self.FL=0
	def readColumns(self, filename):
		self.tables = defaultdict(dict)
		with open(filename, 'r') as TBL:
			cRead = TBL.read().split('\n')
			for line in cRead:
				if '\r' in line: line = line[:len(line)-1]
				if line == '<begin_table>':
					self.table_flag = 1
					pass
				elif line == '<end_table>':
					pass
				else:
					if self.table_flag == 1:
						self.table_name = line
						self.table_flag = 0
					else:
						self.tables[self.table_name][self.table_name+'.'+line] = []
						self.rowOrder[self.table_name].append(self.table_name+'.'+line)
		return self.tables
	def readData(self, filename, tableName):
		with open(filename, 'r') as TBL:
			cRead = TBL.read().split()
			for rows in cRead:
				rows = rows.split(',')
				for number, rowData in enumerate(rows):
					self.tables[tableName][self.rowOrder[tableName][number]].append(int(rowData))
				self.rowTables[tableName].append(rows)
		return self.tables, self.rowTables
	def checkColInTable(self,cols,tableName):
		for col in cols:
			if col not in self.tables[tableName]: return 0
		return 1
	def joinTwo(self, table1, table2,row1,row2):
		starArr=[]
		for i in table1:
			for j in table2:
				starArr.append(i+j)
		tmpRow = row1+row2
		return starArr, tmpRow
	def whereRun(self,cond, allAns, allRow):
		TEMP = []
		if '>=' in cond:
			l,r = cond.split('>=')
			if r not in allRow:
				for i in allAns:
					if (l not in allRow):
						self.FL=1
						break
					if i[allRow.index(l)] >= int(r): TEMP.append(i)
			else:
				for i in allAns:
					if (r not in allRow) or (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) >= int(i[allRow.index(r)]): TEMP.append(i)
		elif '<=' in cond:
			l,r = cond.split('<=')
			if r not in allRow:
				for i in allAns:
					if (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) <= int(r): TEMP.append(i)
			else:
				for i in allAns:
					if (r not in allRow) or (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) <= int(i[allRow.index(r)]): TEMP.append(i)
		elif '!=' in cond:
			l,r = cond.split('!=')
			if r not in allRow:
				for i in allAns:
					if (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) != int(r):
						TEMP.append(i)
			else:
				for i in allAns:
					if (r not in allRow) or (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) != int(i[allRow.index(r)]): TEMP.append(i)
		elif '=' in cond:
			l,r = cond.split('=')
			if r not in allRow:
				for i in allAns:
					if (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) == int(r): TEMP.append(i)
			else:
				for i in allAns:
					if (r not in allRow) or (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) == int(i[allRow.index(r)]): TEMP.append(i)
		
		elif '>' in cond:
			l,r = cond.split('>')
			if r not in allRow:
				for i in allAns:
					if (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) > int(r): TEMP.append(i)
			else:
				for i in allAns:
					if (r not in allRow) or (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) > int(i[allRow.index(r)]) :
						TEMP.append(i)
		elif '<' in cond:
			l,r = cond.split('<')
			if r not in allRow:
				for i in allAns:
					if (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) < int(r): TEMP.append(i)
			else:
				for i in allAns:
					if (r not in allRow) or (l not in allRow):
						self.FL=1
						break
					if int(i[allRow.index(l)]) < int(i[allRow.index(r)]): TEMP.append(i)
		if self.FL == 1:
			print "syntax-error!"
			return []
		return TEMP
	def runAGG(self,cond,tempAns,tempRow):
		ans=0
		ANS=[]
		if 'max' in cond[0]:
			S=cond[0][4:len(cond[0])-1]
			if ',' in S: sys.exit("syntax-error!")
			ans=-10**12
			for i in tempAns:
				if S not in tempRow: sys.exit("syntax-error!")
				ans = max(ans, int(i[tempRow.index(S)]))
			ans=[[ans]]
		if 'min' in cond[0]:
			S=cond[0][4:len(cond[0])-1]
			if ',' in S: sys.exit("syntax-error!")
			ans=10**12
			for i in tempAns:
				if S not in tempRow: sys.exit("syntax-error!")
				ans = min(ans, int(i[tempRow.index(S)]))
			ans=[[ans]]
		if 'sum' in cond[0]:
			S=cond[0][4:len(cond[0])-1]
			if ',' in S: sys.exit("syntax-error!")
			ans=0
			for i in tempAns:
				if S not in tempRow: sys.exit("syntax-error!")
				ans +=int(i[tempRow.index(S)])
			ans=[[ans]]
		if 'avg' in cond[0]:
			S=cond[0][4:len(cond[0])-1]
			if ',' in S: sys.exit("syntax-error!")
			ans=0
			for i in tempAns:
				if S not in tempRow: sys.exit("syntax-error!")
				ans += int(i[tempRow.index(S)])
			if len(tempAns) != 0:ans=ans/float(len(tempAns))
			else: ans=0.0
			ans=[[ans]]
		if 'dis' in cond:
			ans=[]
			S=cond[4:len(cond)-1].split(',')
			temp=[]
			for i in tempAns:
				GG=[]
				for j in S:
					if j not in tempRow: sys.exit("syntax-error!")
					else:GG.append(i[tempRow.index(j)])
				ANS.append(tuple(GG))
				ans = list(set(ANS))
				for i in range(len(ans)): ans[i] = list(ans[i])
		return ans
	def runQuery(self, query):
		query = query.split()
		if len(query) < 4: sys.exit("syntax-error!")
		AG=""
		if query[0].lower() != 'select': sys.exit("syntax-error!")
		if query[2].lower() != 'from': sys.exit("syntax-error!")
		if 'dis' not in query[1]: 
			cols = query[1].split(',')
			if ('min' in cols[0] and len(cols)>1) or ('max' in cols[0] and len(cols)>1) or ('sum' in cols[0] and len(cols)>1) or('dis' in cols[0] and len(cols)>1):sys.exit("syntax-error!")
		else:
			cols=query[1]
			AG="dis"
		tableArr = query[3].split(',')
		self.starAns = self.rowTables[tableArr[0]]
		self.starRow = self.rowOrder[tableArr[0]]
		if tableArr[0] not in self.tables: sys.exit("syntax-error!")
		for i in tableArr[1:]:
			if i not in self.tables: sys.exit("syntax-error!")
			self.starAns, self.starRow = self.joinTwo(self.rowTables[i], self.starAns, self.rowOrder[i] ,self.starRow)
		tempAns = []
		afterWhere=[]
		try:
			if ('and' in query or 'or' in query) and 'where' in query:
				q1 = query[5]
				q2 = query[7]
				a1 = self.whereRun(q1,self.starAns,self.starRow)
				if 'and' in query:a2 = self.whereRun(q2,a1,self.starRow)
				else: 
					a2 = (a1 + self.whereRun(q2,self.starAns,self.starRow))
					for i in range(len(a2)): a2[i]=tuple(a2[i])
					a2=list(set(a2))
					for i in range(len(a2)): a2[i]=list(a2[i])
			elif 'where' in query:
				q1 = query[5]
				a2 = self.whereRun(q1,self.starAns,self.starRow)
			else:
				a2 = [self.starAns[i] for i in range(len(self.starAns))]
			afterWhere = a2
		except:pass
		finalAns=[]
		for it in self.aggregate:
			if it in cols[0]:
				AG=it
				break
		if AG == "":
			if cols[0] == '*':cols = self.starRow
			for k,i in enumerate(afterWhere):
				X=[]
				for j in cols:
					# print j
					if j not in self.starRow: sys.exit("syntax-error!")
					X.append(afterWhere[k][self.starRow.index(j)])
				finalAns.append(X)
		else:
			finalAns = self.runAGG(cols,afterWhere,self.starRow)
		if 'dis' in cols:
			cols = cols[4:len(cols)-1].split(',')
		return finalAns,cols,self.FL
