import os
import pyodbc
import redis
import sys
import timeit
import hashlib
import pickle
from flask import Flask,redirect,render_template,request
from time import time

############################
### PYODBC CONNECTIVITY ####
############################
server = 'assignment01.database.windows.net'
database = 'assignment01'
username = 'supreetha'
password = 'Chuppi$123'
driver= '{ODBC Driver 17 for SQL Server}'
cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()
r = redis.StrictRedis(host='assignment01.redis.cache.windows.net',port=6380, db=0, password='O8IrxXBklv50PJZ3IS9kefYiac3naKdqrAzCaCHdmP0=', ssl=True)
result = r.ping()
print("Ping returned : " + str(result))

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')


#query10
@app.route('/range')
def range_query():
    elevation1 = str(request.args.get('ele1'))
    elevation2 = str(request.args.get('ele2'))
    volrange1 = str(request.args.get('vol1'))
    volrange2 = str(request.args.get('vol2'))

    cursor.execute("SELECT volcano_name, country, region, latitude, longitude, elev FROM volcano WHERE number >= "+volrange1+" and number <= "+volrange2+" and elev >= "+elevation1+" and elev <= "+elevation2+"; ")
    output = cursor.fetchall()
    cursor.execute("SELECT min(elev) FROM volcano WHERE number >= "+volrange1+" and number <= "+volrange2+" and elev >= "+elevation1+" and elev <= "+elevation2+" ; ") 
    result = cursor.fetchall()
    for i in result:
        result = i[0]
    cursor.execute("SELECT max(elev) FROM volcano WHERE number >= "+volrange1+" and number <= "+volrange2+" and elev >= "+elevation1+" and elev <= "+elevation2+" ; ")
    result2 = cursor.fetchall()
    for j in result2:
        result2 = j[0]
    return render_template('range.html', output = output, result = result, result2 =result2) 


#query 11
@app.route("/sequenceRange" , methods=['GET','POST'])
def seqrange():
    seqrange1 = str(request.args.get('range1'))
    seqrange2 = str(request.args.get('range2'))
    starttime = timeit.default_timer()
    cursor.execute("select volcano_name, country, region, latitude, longitude, elev from volcano where number in (select number from volcanoindex where sequence >= "+seqrange1+" and sequence <= "+seqrange2+");")
    rangevalues = cursor.fetchall()
    timee = timeit.default_timer() - starttime
    return render_template('sequenceRange.html', rangeresult = rangevalues, timeelapsed = timee)

#query 11b
@app.route("/sequenceRange2" , methods=['GET','POST'])
def sequencerangequery2():
 getrangeval = str(request.args.get('srangeval'))
 starttime = timeit.default_timer()
 cursor.execute("select top "+getrangeval+" volcano_Name, country, region, latitude, longitude, elev from volcano order by number desc;")
 rangeresult = cursor.fetchall()
 time_elapsed = timeit.default_timer() - starttime
 return render_template('sequenceRange2.html', rangeoutputval=rangeresult, timeelapsed = time_elapsed)

#query 12
@app.route('/query7')
def query7_withoutcache():
    number1 = str(request.args.get('number1'))

    start = time()
    for z in range(int(number1)):
        cursor.execute("select volcano_name, country, region, latitude, longitude, elev from volcano where number in (select number from volcanoindex where sequence >= 1000 and Sequence <= 1500);")
        values = cursor.fetchall()
        cursor.execute("select top 5 volcano_Name, country, region, latitude, longitude, elev from volcano order by number desc;")
        result = cursor.fetchall()

    end = time()
    total = end - start
    return render_template('query7.html', timetaken = total) 

#query 13
@app.route('/query8')
def query2withcache():
    randcachenum = str(request.args.get('number2'))
    sql_qr1 = "select volcano_name, country, region, latitude, longitude, elev from volcano where number in (select number from volcanoindex where sequence >= 1000 and Sequence <= 1500);"
    sql_qr2 = "select top 5 volcano_Name, country, region, latitude, longitude, elev from volcano order by number desc;"
    
    hash1 = hashlib.sha224(sql_qr1.encode('utf-8')).hexdigest()
    hash2 = hashlib.sha224(sql_qr2.encode('utf-8')).hexdigest()

    redis1 = 'redis_cache1:' + hash1
    redis2 = 'redis_cache2:' + hash2
    startcount = time()
    
    for qry in range(int(randcachenum)):
        if(r.get(redis1) and r.get(redis2)):
            print("CACHING REDIS")
        else:
            print("Inside else")
            cursor.execute(sql_qr1)
            cursor.execute(sql_qr2)
            outputcachedres = cursor.fetchall()
            r.set(redis1, pickle.dumps(outputcachedres))
            r.set(redis2, pickle.dumps(outputcachedres))
            r.expire(redis1, 60)
            r.expire(redis2, 60)
        
    endcount = time()
    totcount = endcount - startcount
    return render_template("query8.html", enhancedrendertime = totcount)

if __name__ == "__main__":
    app.run()