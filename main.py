from sys import argv
import csv
import re #for regex search
import itertools
from copy import deepcopy
from prettytable import PrettyTable

# Global Variables
conditionals = [">",">=","<","<=","="]
agg = ['max','sum','avg','min']
cond_join = False
lg, rg = [],[]

flatten = lambda l: [item for metadata in l for item in metadata]
def readFile(name):
    """ Function to read the structure of the table """
    fileData = []
    with open(name,'r') as f:
            reader = csv.reader(f)
            fileData = [row for row in reader]
    return flatten(fileData)

def createData(structure):
    """ Reads Data into memory """
    btable = []
    etable =[]
    for index,tags in enumerate(structure):
        if re.search("begin_table*",tags):
            btable.append(index)
        if re.search("end_table*",tags):
            etable.append(index)

    t = [] 
    for i in range(len(btable)):
        t.append([btable[i],etable[i]])

    columns = {}
    for b,e in t:
        name = structure[b+1]
        columns[name] = [structure[b+j] for j in range((e-b))][2:]
    
    final = {} 
    for tb,coly in columns.items():
        t1 = {}
        name = tb + '.csv'
        tname = readFile(name)
        t1['col'] = [tb+'.'+c for c in coly]
        t1['row'] =[]
        length = len(t1['col'])
        for i in range(0,len(tname),length):
            try:
                temp = [int(g) for g in tname[i:i+length]]
            except ValueError:
                temp = [float(g) for g in tname[i:i+length]]
            t1['row'].append(temp)
        final[tb] = t1 
    return final

def parser():
    """ Parses query and gives out table,column and conditions """
    if argv[1][-1] == ';':
        querry = argv[1][:-1]
    else:
        print("Incorrect query. Missing semi colon")
        exit()
    if 'select' not in querry:
        print('Incorrect query. Command not supported')
        exit()
    if 'from' not in querry:
        print('Incorrect query. Command not supported')
        exit()
    columns = []
    where =[]
    tables =[]

    if 'distinct' in querry:
        columns = querry.split('select ')[1].split(' from')[0].split('distinct ')[1].split(',')
    else:
        columns = querry.split('select ')[1].split(' from')[0].split(',')
    temp = []
    aggy = []
    count = 0
    flag =0
    for fn in agg:
        for c in columns:
            if fn in c:
                flag =1
                count+=1
                temp.append(c.split('(')[1].split(')')[0])
                aggy.append(fn)
    if flag == 1: 
        if count != len(columns):
            print("Incorrect querry. Aggregate functions cannot be used with normal querry")
            exit()
        else:
            columns = temp  
    
    if 'where' in querry: 
        tables = querry.split('select ')[1].split(' from ')[1].split(' where')[0].split(',')
        where = querry.split('select ')[1].split(' from ')[1].split(' where ')[1].split(',')
    else:
        tables = querry.split('select ')[1].split(' from ')[1].split(',')
    return tables,columns,where,querry,aggy

def crossproduct(tables):
    """ Takes the cross product of the tables in the querry """
    table_columns = []
    table_rows = []
    for table in tables:

        if table not in data.keys():
            print("Incorrect query. Table "+table+" not present")
            exit()
        else:
            table_columns.append(data[table]['col'])
            if len(table_rows) == 0:
                table_rows.append(data[table]['row'])
            else:
                temp_table = list(itertools.product(table_rows,data[table]['row'])) #crossproduct with previous table
                final = []
                for j in range(len(data[table]['row'])):
                    g= deepcopy(temp_table[j][0])
                    for l in g:
                        for i in temp_table[j][1]:
                            l.append(i)
                    final.append(g)
                table_rows = deepcopy(final)
    return flatten(table_columns),flatten(table_rows),len(data[table]['row'])

def convertType(num):
    """ Converts string into int or float """
    try:
        return int(num)
    except ValueError:
        return float(num)

def checkType(num):
    """ Check if string is a number """
    try:
        return convertType(num)
    except ValueError:
        return "str"

def cond_check(left,sign,right):
    """ Checks for condition satisfaction between 2 variables """
    right = convertType(right)
    if sign == "=":
        return (left==right)
    if sign == ">":
        return (left>right)
    if sign == "<":
        return (left<right)
    if sign == ">=":
        return (left>=right)
    if sign == "<=":
        return (left<=right)

def joinConditional(condition, table_columns, table_rows):
    """ Returns column indexes for comparision between 2 columns """
    global cond_join,lg,rg
    lefty,righty = -1,-1
    left, sign, right = condition
    
    # error checking
    for index, col in enumerate(table_columns):
        if left == col or left == col.split('.')[-1]:
            if lefty == -1:
                lefty = index
            else:
                print("Incorrect querry. Ambiguous condition")
                exit()
        if right == col or right == col.split('.')[-1]:
            if righty == -1:
                righty = index
            else:
                print("Incorrect querry. Ambiguous condition")
                exit()

    if lefty ==  -1 or righty == -1:
        print("Incorrect querry. Condition columns not present in the table")
        exit()

    if sign == '=':
        cond_join = True
        lg.append(lefty)
        rg.append(righty)
    return lefty,righty

def reverseOp(ch):
    """ Reverses sign of operator """
    if ch == "<":
        return ">"
    elif ch == ">":
        return "<"
    elif ch == ">=":
        return "<="
    elif ch == "<=":
        return ">="
    return ch

def reverseSign(sign):
    """ Extends the previous function """
    tmp =""
    for ch in sign:
        tmp = tmp+reverseOp(ch)
    return tmp

def conditioning(table_columns,table_rows,cond1,cond2,add_type):
    """ Returns valid rows pertaining to 2 conditions """
    global lg,rg,cond_join
    if len(cond1.split(' ')) != 3:
        print("Incorrect querry. Format of condition is incorrect")
        exit()
    left_cond1, sign_cond1, right_cond1 = cond1.split(' ')
    sign_cond1 = sign_cond1.lower()
    if cond2 == '':
        cond2 = cond1
        add_type = 'and'
    else:
        if len(cond2.split(' ')) != 3:
            print("Incorrect querry. Format of condition is incorrect")
            exit()
    left_cond2, sign_cond2, right_cond2 = cond2.split(' ')
    sign_cond2 = sign_cond2.lower()
    left1_type,right1_type = checkType(left_cond1), checkType(right_cond1)
    left2_type,right2_type = checkType(left_cond2), checkType(right_cond2)
    l1,r1,l2,r2 = -1,-1,-1,-1
    flagc1,flagc2,flagn1,flagn2 = 0,0,0,0
    if left1_type == right1_type:
        if left1_type != 'str':
            print("Incorrect querry. Condition format not supported")
            exit()
        else:
            l1,r1 = joinConditional(cond1.split(' '), table_columns, table_rows)
            flagc1 = 1
            # return t,cols
    if left2_type == right2_type:
        if left2_type != 'str':
            print("Condition format not supported")
            exit()
        else:
            l2,r2 = joinConditional(cond2.split(' '), table_columns, table_rows)
            flagc2 = 1
    if right1_type != 'str':
        flagn1 = 1
    if left1_type != 'str':
        left_cond1 , right_cond1 = right_cond1,left_cond1
        sign_cond1 = reverseSign(sign_cond1)
        flagn1 = 1
    if right2_type != 'str':
        flagn2=1
    if left2_type != 'str':
        left_cond2 , right_cond2 = right_cond2,left_cond2
        sign_cond2 = reverseSign(sign_cond2)
        flagn2 = 1
    
    ind1,ind2 = -1,-1
    if cond1 == cond2:
        if cond_join:
            lg = [lg[0]]
            rg = [rg[0]]
    for index, col in enumerate(table_columns):
        if flagn1:
            if left_cond1 == col or left_cond1 == col.split('.')[-1]:
                if ind1 == -1:
                    ind1 = index
                else:
                    print("Incorrect querry. Ambiguous condition")
                    exit()
        if flagn2:
            if left_cond2 == col or left_cond2 == col.split('.')[-1]:
                if ind2 == -1:
                    ind2 = index
                else:
                    print("Incorrect querry. Ambiguous condition")
                    exit()
        
    if (ind1 == -1 and flagn1 == 1) or (ind2 == -1 and flagn2 ==1):
        print("Incorrect querry. Condition variables missing")
        exit()
    
    temp = []
    if add_type == 'and':
        for row in table_rows:
            if flagc1 and flagc2:
                if cond_check(row[l1],sign_cond1,row[r1]) and cond_check(row[l2],sign_cond2,row[r2]):
                    temp.append(row)
            if flagc1 and flagn2:
                if cond_check(row[l1],sign_cond1,row[r1]) and cond_check(row[ind2],sign_cond2,right_cond2):
                    temp.append(row)
            if flagn1 and flagc2:
                if cond_check(row[ind1],sign_cond1,right_cond1) and cond_check(row[l2],sign_cond2,row[r2]):
                    temp.append(row)
            if flagn1 and flagn2:
                if cond_check(row[ind1],sign_cond1,right_cond1) and cond_check(row[ind2],sign_cond2,right_cond2):
                    temp.append(row)
    
    if add_type == 'or':
        for row in table_rows:
            if flagc1 and flagc2:
                if cond_check(row[l1],sign_cond1,row[r1]) or cond_check(row[l2],sign_cond2,row[r2]):
                    temp.append(row)
            if flagc1 and flagn1:
                if cond_check(row[l1],sign_cond1,row[r1]) or cond_check(row[ind2],sign_cond2,right_cond2):
                    temp.append(row)
            if flagn1 and flagc2:
                if cond_check(row[ind1],sign_cond1,right_cond1) or cond_check(row[l2],sign_cond2,row[r2]):
                    temp.append(row)
            if flagn1 and flagn2:
                if cond_check(row[ind1],sign_cond1,right_cond1) or cond_check(row[ind2],sign_cond2,right_cond2):
                    temp.append(row)
    return table_columns, temp

def getCol(table,index):
    """ Returns column """
    try:
        return list(zip(*table))[index]
    except IndexError:
        return []

def colSelect(table,table_columns,table_rows,columns):
    """ For given rows extracts columns """
    tmp_table =[]
    copy_attr = []
    if columns[0] == "*":
        columns = table_columns
    for col in columns:
        if col.lower == 'distinct':
            continue
        count = 0
        if '.' in col:
            temp=col
        else:
            for coly in table_columns:
                if col == coly.split('.')[-1]:
                    temp = str(coly.split('.')[0])+'.'+col
                    count += 1
            if count > 1:
                print("Incorrect querry. Column present in multiple tabels.")
                exit()
            elif count == 0:
                print("Incorrect querry. Column missing")
                exit()
        copy_attr.append(temp)
        if table_columns.count(temp) == 0:
            print("Incorrect querry. Column missing")
            exit()
        index = table_columns.index(temp)
        tmp_table.append(getCol(table_rows,index))
    return tmp_table, copy_attr

def distinctQuerry(table_rows):
    """ Distinct tuples """
    d = []
    for t in table_rows:
        if t not in d:
            d.append(t)
    return d    

def aggCol(aggy,cols,tcols):
    """ Aggregate columns """
    temp = []
    temp_cols =[]
    for idx,col in enumerate(cols):
        cond = aggy[idx]
        if cond == 'max':
            temp.append(max(col))
            temp_cols.append('max('+tcols[idx]+')')
        if cond == 'min':
            temp.append(min(col))
            temp_cols.append('min('+tcols[idx]+')')
        if cond == 'sum':
            temp.append(sum(col))
            temp_cols.append('sum('+tcols[idx]+')')
        if cond == 'avg':
            temp.append(sum(col)/len(col))
            temp_cols.append('avg('+tcols[idx]+')')
    return temp_cols,temp

def condy(table_columns,final,querry):
    """ Removes columns in case of conditional join """
    global cond_join, lg,rg
    star = '*' in querry.split()
    flag = 0
    if star and cond_join:
        for i,l in enumerate(lg):
            r = rg[i]
            flag = max(l,r)
            del table_columns[flag]
            for j in range(len(final)):
                del final[j][flag]
    return table_columns,final

def makeTable(table_columns,table_cross,aggy,where,columns):
    """ Returns sql query """
    add_type = None
    if len(where) > 0:
        cond1,cond2 = '',''
        ifand = where[0].split(' and ')
        ifor = where[0].split(' or ')
        if len(ifand) == 2:
            cond1 = ifand[0]
            cond2 = ifand[1]
            add_type = 'and'
        elif len(ifor) == 2:
            cond1 = ifor[0]
            cond2 = ifor[1]
            add_type = 'or'
        else:
            cond1 = where[0]
        table_columns,table_cross = conditioning(table_columns,table_cross,cond1,cond2,add_type)

    tmp_table, attributes = colSelect(tables,table_columns,table_cross,columns)
    final = []

    for row in list(zip(*tmp_table)):
        final.append(list(row))
    if "distinct" in querry.lower().split():
        final = distinctQuerry(final)

    attributes,final = condy(attributes,final,querry)

    if len(aggy) != 0:
        p =list(zip(*final))
        attributes,final = aggCol(aggy,p,attributes)
        temp = []
        temp.append(final)
        final = temp

    x = PrettyTable()
    x.field_names = attributes
    for row in final:
        x.add_row(row)
    print(x)



metadata = readFile('metadata.txt')
data = createData(metadata)
tables,columns,where,querry,aggy = parser()
table_columns,table_cross,length = crossproduct(tables)
makeTable(table_columns,table_cross,aggy,where,columns)