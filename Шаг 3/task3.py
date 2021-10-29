import json

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol

from resolver_functions import *

not_duplicate = [6]
attributes = ['signature_date', 'Name', 'PayWay', 'Address', 'ValidUniversalServicesCard', 
		'ID', 'IntercityConnectionPayment', 'global_id', 'Latitude_WGS84', 'duplicate_id']


class DataFusion(MRJob):

    def mapper_raw(self, input_path, input_uri):
        IS_dataMos = 'dataMosutf.json' in input_path
        with open(input_path, 'r', encoding='utf-8-sig') as file:
            data = json.load(file)
        for obj in data:
            if 'duplicate_id' not in obj.keys():
                not_duplicate[0] += 1
                yield not_duplicate, obj
            else:
                yield obj['duplicate_id'], (obj, 'dataMosutf.json' if IS_dataMos else 'dataGovutf.json')

    def reducer(self, dup_id, objs):
        objs = [*objs]
        if len(objs) == 1:
            with open('/home/alisher/Data_integration/Task3/out.json', 'a') as out_file:
                out_file.write(json.dumps(objs[0], indent = 2, ensure_ascii = False) + '\n')
        else:
            obj1, src1, obj2, src2 = objs[0][0], objs[0][1], objs[1][0], objs[1][1]
            for attribute in attributes:
                val1, val2 = obj1.get(attribute), obj2.get(attribute)
                obj1[attribute] = resolve(vals = [val1, src1, val2, src2], resolve_fun = resolver_funcs[attribute])
            del obj1['duplicate_id']
            with open('/home/alisher/Data_integration/Task3/out.json', 'a') as out_file:
                out_file.write(json.dumps(obj1, indent=4, ensure_ascii=False) + '\n')


    def steps(self):
        return [
            MRStep(
                mapper_raw=self.mapper_raw,
                reducer=self.reducer
            )
        ]


if __name__ == '__main__':
    DataFusion.run()
