import random
import string
import json
from json import JSONEncoder
import datetime

companyname = []
for k in range(1,6):
    companyname.append("".join(random.sample(string.ascii_lowercase,5)))


output = open('input_json_short.txt',"w")

for k in range(1,1001):
    #depport = random.choice(portname)
    depport = random.randint(1,50)
    arrport = random.randint(1,50)
    #random.choice(portname)

    while(depport==arrport):
        arrport = random.randint(1,50)

    value = random.uniform(25,50)
    provider = random.choice(companyname)
    chosenday = random.randint(1,31)
    dt = datetime.datetime(2015, 1, chosenday, 0, 0)
    timeval = dt.timestamp()
    #date1 = date.fromtimestamp(int(dt.timestamp()))
    #date1.day

    jsonstring = JSONEncoder().encode({
        "departureId":depport,
        "arrivalId":arrport,
        "provider":provider,
        "timestamp":timeval,
        "value":value
        })
    output.write(jsonstring)
    output.write('\n')

output.close()


