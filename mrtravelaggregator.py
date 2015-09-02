from itertools import groupby
from operator import itemgetter
import json
from collections import defaultdict
import datetime
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

class MRTravelAggregator(MRJob):
    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol
    #file = open('input_file.json','r')
    def mapper(keyin,file):
            input_data=[]
            for line in file:
                line = json.loads(line[:len(line)-1])
                date1=  datetime.datetime.fromtimestamp(int(line["timestamp"]))            
                line["days_in_month"] = date1.day
                input_data.append(line)
                
            grouper = itemgetter("departureId","arrivalId","provider","days_in_month")
            
            
            for key, grp in groupby(sorted(input_data, key = grouper), grouper):
                    val_dict["value"] = []
                    val_dict["day_in_month"] = []
                    key_dict = dict(zip(["departureId","arrivalId","provider"], key[:3]))
                    val_dict["value"].append(sum(item["value"] for item in grp))
                    val_dict["day_in_month"]=str(key)
                    
                    yield key_dict,val_dict

    def reducer(key_dict,val_dict):
        
        grouper = itemgetter("day_in_month")
        series = []
        if len(val_dict)>1:
            for key, grp in groupby(sorted(val_dict, key = grouper), grouper):
                temp_json = {}#defaultdict()
                temp_json["day_in_month"] = key
                temp_json["sumOfValues"] = sum(item["value"][0] for item in grp)        
                series.append(temp_json)
        else:
             temp_json ={} #defaultdict()
             temp_json["day_in_month"] = val_dict["day_in_month"]
             temp_json["sumOfValues"] = val_dict["value"]     
             series.append(temp_json)
        #temp_dict = defaultdict()
        key_dict["series"] = series
        
        yield key_dict
        
if __name__=='__main__':
    MRTravelAggregator.run()
