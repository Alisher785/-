import json

from mrjob.job import MRJob
from mrjob.step import MRStep

from edit_distance import edit_distance

duplicat_number = [0]


class FindDups(MRJob):

    def mapper_raw(self, input_path, input_uri):
        with open(input_path, 'r', encoding='utf-8-sig') as file:
            data = json.load(file)
        for obj in data:
            key = obj['Address']
            key = ' '.join(key.lower().split(',')[0].strip())
            yield hash('-'.join([key])), obj


    def is_duplicate(self, obj1, obj2):
        address_1 = obj1['Address'].replace(',', '')
        address_2 = obj2['Address'].replace(',', '')
        id_sum1, id_sum2 = 0, 0
        n_1 = obj1['global_id']
        n_2 = obj2['global_id']
        while n_1 > 0:
            id_sum1 += n_1 % 10
            n_1 = n_1 // 10
        while n_2 > 0:
            id_sum2 += n_2 % 10
            n_2 = n_2 // 10
        return edit_distance(address_1, address_2)[0] < 3 and (abs(id_sum2 - id_sum1 <= 9))


    def reducer(self, _, objs):
        objs = list(objs)
        for i, obj_i in enumerate(objs):
            for j in range(i + 1, len(objs)):
                obj_j = objs[j]
                if self.is_duplicate(obj_i, obj_j):
                    obj_i['duplicate_id'], obj_j['duplicate_id'] = duplicat_number[0], duplicat_number[0]
                    duplicat_number[0] += 1
                    with open('/home/alisher/Data_integration/Task2/out.json', 'a') as out_file:
                        out_file.write(json.dumps(obj_j, indent = 2, ensure_ascii = False) + '\n')
                        out_file.write(json.dumps(obj_i, indent = 2, ensure_ascii = False) + '\n')

    def steps(self):
        return [
            MRStep(
                mapper_raw = self.mapper_raw,
                reducer = self.reducer
            )
        ]


if __name__ == '__main__':
    FindDups.run()
