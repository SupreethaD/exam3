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


#query5a without cache
@app.route('/range')
def range_query():
    elevation1 = str(request.args.get('ele1'))
    elevation2 = str(request.args.get('ele2'))
    volrange1 = str(request.args.get('vol1'))
    volrange2 = str(request.args.get('vol2'))

    cursor.execute("SELECT volcano_name, country, region, latitude, longitude, elev FROM v WHERE number >= "+volrange1+" and number <= "+volrange2+" and elev >= "+elevation1+" and elev <= "+elevation2+"; ")
    output = cursor.fetchall()
    cursor.execute("SELECT avg(elev) FROM v WHERE number >= "+volrange1+" and number <= "+volrange2+" and elev >= "+elevation1+" and elev <= "+elevation2+" group by elev; ")
    
    result = cursor.fetchall()
    for i in result:
        result = i[0]
    return render_template('range.html', output = output, result = result) 


#query 6a
@app.route("/sequenceRange" , methods=['GET','POST'])
def seqrange():
    seqrange1 = str(request.args.get('range1'))
    seqrange2 = str(request.args.get('range2'))
    cursor.execute("select volcano_name, country, region, latitude, longitude, elev from v where number in (select number from vindex where sequence >= "+seqrange1+" and Sequence <= "+seqrange2+");")
    rangevalues = cursor.fetchall()
    return render_template('sequenceRange.html', rangeresult = rangevalues)

#query 6b
@app.route("/sequenceRange2" , methods=['GET','POST'])
def sequencerangequery2():
 getrangeval = str(request.args.get('srangeval'))
 starttime = timeit.default_timer()
 cursor.execute("select top "+getrangeval+" volcano_Name, country, region, latitude, longitude, elev from v order by number desc;")
 rangeresult = cursor.fetchall()
 time_elapsed = timeit.default_timer() - starttime
 return render_template('sequenceRange2.html', rangeoutputval=rangeresult, timeelapsed = time_elapsed)

#query7 without cache
@app.route('/query7witoutcache')
def query7_withoutcache():
    inputnumber = str(request.args.get('numberfield1'))

    startrange = '100'
    endrange = '100000'
    startelevation = '100'
    endelevation = '100000'
    starttime = time()
    for z in range(int(inputnumber)):
        cursor.execute("SELECT volcano_name, country, region, latitude, longitude, elev FROM v WHERE number >= "+startrange+" and number <= "+endrange+" and elev >= "+startelevation+" and elev <= "+endelevation+"; ")
        output = cursor.fetchall() 
        cursor.execute("SELECT avg(elev) FROM v WHERE number >= "+startrange+" and number <= "+endrange+" and elev >= "+startelevation+" and elev <= "+endelevation+" group by elev; ")
        output11 = cursor.fetchall()
        cursor.execute("select volcano_name, country, region, latitude, longitude, elev from v where number in (select number from vindex where sequence >= 1000 and Sequence <= 1500);")
        rangevalues = cursor.fetchall()
        cursor.execute("select top 5 volcano_Name, country, region, latitude, longitude, elev from v order by number desc;")
        rangeresult = cursor.fetchall()

    endtime = time()
    totaltime = endtime - starttime
    return render_template('query7witoutcache.html', timetaken = totaltime) 

#query8 with cache
@app.route('/query8withcache')
def query2withcache():
    randcachenum = str(request.args.get('numberfield1'))
    startrange = '100'
    endrange = '100000'
    startelevation = '100'
    endelevation = '100000'
    sql_qr1 = "SELECT volcano_name, country, region, latitude, longitude, elev FROM v WHERE number >= "+startrange+" and number <= "+endrange+" and elev >= "+startelevation+" and elev <= "+endelevation+";"
    sql_qr2 = "SELECT avg(elev) FROM v WHERE number >= "+startrange+" and number <= "+endrange+" and elev >= "+startelevation+" and elev <= "+endelevation+" group by elev;"
    sql_qr3 = "select volcano_name, country, region, latitude, longitude, elev from v where number in (select number from vindex where sequence >= 1000 and Sequence <= 1500);"
    sql_qr4 = "select top 5 volcano_Name, country, region, latitude, longitude, elev from v order by number desc;"
    
    hash_val1 = hashlib.sha224(sql_qr1.encode('utf-8')).hexdigest()
    hash_val2 = hashlib.sha224(sql_qr2.encode('utf-8')).hexdigest()
    hash_val3 = hashlib.sha224(sql_qr3.encode('utf-8')).hexdigest()
    hash_val4 = hashlib.sha224(sql_qr4.encode('utf-8')).hexdigest()
    redis_ky1 = 'redis_cache1:' + hash_val1
    redis_ky2 = 'redis_cache2:' + hash_val2
    redis_ky3 = 'redis_cache3:' + hash_val3
    redis_ky4 = 'redis_cache4:' + hash_val4
    startcount = time()
    
    for qry in range(int(randcachenum)):
        if(r.get(redis_ky1) and r.get(redis_ky2) and r.get(redis_ky3) and r.get(redis_ky4)):
            print("CACHING REDIS")
        else:
            print("Inside else")
            cursor.execute(sql_qr1)
            cursor.execute(sql_qr2)
            cursor.execute(sql_qr3)
            cursor.execute(sql_qr4)
            outputcachedres = cursor.fetchall()
            r.set(redis_ky1, pickle.dumps(outputcachedres))
            r.set(redis_ky2, pickle.dumps(outputcachedres))
            r.set(redis_ky3, pickle.dumps(outputcachedres))
            r.set(redis_ky4, pickle.dumps(outputcachedres))
            r.expire(redis_ky1, 60)
            r.expire(redis_ky2, 60)
            r.expire(redis_ky3, 60)
            r.expire(redis_ky4, 60)
        
    endcount = time()
    totcount = endcount - startcount
    return render_template("query8withcache.html", enhancedrendertime = totcount)

if __name__ == "__main__":
    app.run()