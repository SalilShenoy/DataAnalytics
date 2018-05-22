import json
import pymongo
import matplotlib.pyplot as plt

class Mongo_Connection(object):

    def __init__(self):
        self.mongo_uri = 'mongodb://<user>:<password>@ds229690.mlab.com:29690/dep'
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client.get_database('dep')
        self.dbcollection = self.db.get_collection('demographics')
        if self.dbcollection is None:
            self.dbcollection = self.db.create_collection('demographics')

    def write_to_db(self, data):
        if self.db is not None:
            self.dbcollection.insert_many(data)

    def get_database(self):
        return self.db


class DataParser(object):

    def __init__(self):
        self.data = None
        self.column_names = []

    def read_data(self):
        print ('reading data...')
        with open('rows.json') as json_data:
            raw_data = json.load(json_data)
            self.data = raw_data['data']

            for column_data in raw_data['meta']['view']['columns']:
                self.column_names.append(column_data['fieldName'])

    def get_data(self):
        return self.data

    def print_data(self):
        print ('=========================== Dataset Columns ===============================')
        print (self.column_names)
        print ('=========================== Dataset =======================================')
        print (self.data)

    def parse_json(self):
        if self.data is None:
            return
        retval = []
        for i in range(len(self.data)):
            retval.append(dict(zip(self.column_names, self.data[i])))
        return retval

class DataAnalysis(object):

    def __init__(self, database):
        self.database = database
        self.analyze_data = None
        self.opr_list = ['count', 'exit', 'gender', 'ethnicity']

    def report(self, operation):

        if operation is None or operation not in self.opr_list:
            print ('Enter a valid operation...')
            return

        print ('=========================== Reports =======================================')
        if operation == 'exit':
            print ('Exiting the system...')
            return 0
        elif operation == 'count':
            collection = self.database.get_collection('demographics')
            ps = collection.find({}, {'count_participants': True})
            total_participants = 0
            for p in ps:
                total_participants = total_participants + int(p['count_participants'])
            print ('Total Number of Participants is:' + str(total_participants))
        elif operation == 'gender':
            collection = self.database.get_collection('demographics')
            ps = collection.find({}, {'count_female': True, 'count_male': True})
            females = 0
            males = 0
            for p in ps:
                females = females + int(p['count_female'])
                males = males + int(p['count_male'])

            x = ['Male', 'Female']
            x_pos = [i for i, _ in enumerate(x)]
            count = [males, females]

            plt.bar(x_pos, count, color = 'green')
            plt.xticks(x_pos, x)
            plt.ylabel('Count')
            plt.title("Male/Female participation")
            plt.show()
        elif operation == 'ethnicity':
            collection = self.database.get_collection('demographics')
            ps = collection.find({}, {'count_pacific_islander': True,
                                      'count_hispanic_latino': True,
                                      'count_american_indian': True,
                                      'count_asian_non_hispanic': True,
                                      'count_white_non_hispanic': True,
                                      'count_black_non_hispanic': True,
                                      'count_other_ethnicity': True})

            pacific_islander = 0
            hispanic_latino = 0
            american_indian = 0
            asian_non_hispanic = 0
            white_non_hispanic = 0
            black_non_hispanic = 0
            other_ethnicity = 0

            for p in ps:
                pacific_islander = pacific_islander + int(p['count_pacific_islander'])
                hispanic_latino = hispanic_latino + int(p['count_hispanic_latino'])
                american_indian = american_indian + int(p['count_american_indian'])
                asian_non_hispanic = asian_non_hispanic + int(p['count_asian_non_hispanic'])
                white_non_hispanic = white_non_hispanic + int(p['count_white_non_hispanic'])
                black_non_hispanic = black_non_hispanic + int(p['count_black_non_hispanic'])
                other_ethnicity = other_ethnicity + int(p['count_other_ethnicity'])

            x = ['Pacific Islander', 'Hispanic Latino', 'American Indian', 'Asian Non Hispanic', 'White Non Hispanic',
                 'Black Non Hispanic', 'Other Ethnicity']
            x_pos = [i for i, _ in enumerate(x)]
            count = [pacific_islander, hispanic_latino, american_indian, asian_non_hispanic, white_non_hispanic,
                     black_non_hispanic, other_ethnicity]

            plt.bar(x_pos, count, color='green')
            plt.xticks(x_pos, x)
            plt.ylabel('Count')
            plt.title("Ethnicity participation")
            plt.show()

        return 1


def main():
    data_parser = DataParser()
    data_parser.read_data()
    store_data = data_parser.parse_json()
    data_parser.print_data()

    mongo_object = Mongo_Connection()
    if False:
        mongo_object.write_to_db(store_data)
    db = mongo_object.get_database()

    data_analysis = DataAnalysis(db)
    while True:
        report = raw_input('Enter the choice of report: ')
        retval = data_analysis.report(report)
        if retval == 0:
            break

if __name__ == '__main__':
    main()
