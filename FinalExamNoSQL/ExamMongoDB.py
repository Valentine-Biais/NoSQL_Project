from pymongo import MongoClient
import requests
import json
import pprint


def main():
    client = MongoClient('mongodb://localhost:27017')
    db = client['datacorona']
    datacorona = db['locations']

    with open('coronadata.json') as f:
        json_list = json.load(f)

    # New data from the stream API
    r = requests.get('https://coronavirus-tracker-api.herokuapp.com/v2/locations').text
    locations = json.loads(r)['locations']

    datacorona.delete_many({})

    for location in locations: 
        # Insert new elements in the database
        datacorona.insert_one(location)

    while(True):
        print('Hello, this program analyze the evolution of coronavirus in the world') 
        print('Please choose a task to run') 
        print('1. Update the database')
        print('2. Number of countries affected by the coronavirus')
        print('3. Five countries with the most...')
        print('4. Countries with at least one province with more than 100 deaths and less than 100 recovered')
        print('5. Number of coronavirus cases unresolved in the world')

        choice = int(input())

        while(choice != 1 and choice != 2 and choice != 3 and choice != 4 and choice != 5):
            print("Invalid input, please choose a number between 1 and 5")
            choice = int(input())

        if choice == 1:
            datacorona, json_list = update_database(datacorona, json_list)

        elif choice == 2:
            results = datacorona.distinct('country')
            print('There are', len(results), 'countries affected by coronavirus in the world.')

        elif choice == 3:
            print('Five countries with the most...')
            print('1. Currently affected people') 
            print('2. Coronavirus deaths')
            print('3. Recovered people')

            answer = int(input())

            while(answer != 1 and answer != 2 and answer != 3):
                print("Invalid input, please choose a number between 1 and 3")
                answer = int(input())

            if answer == 1:
                pipe = [{'$group' : {'_id':'$country', 'confirmed_cases':{'$sum': '$latest.confirmed'}}},{'$sort' : {'confirmed_cases': -1, '_id': 1}}, {'$limit': 5}]
                results = datacorona.aggregate(pipeline=pipe)
                for result in results:
                    pprint.pprint(result)

            elif answer == 2:
                pipe = [{'$group' : {'_id':'$country', 'deaths_cases':{'$sum': '$latest.deaths'}}},{'$sort' : {'deaths_cases': -1, '_id': 1}}, {'$limit': 5}]
                results = datacorona.aggregate(pipeline=pipe)
                for result in results:
                    pprint.pprint(result)

            elif answer == 3:
                pipe = [{'$group' : {'_id':'$country', 'recovered_cases':{'$sum': '$latest.recovered'}}},{'$sort' : {'recovered_cases': -1, '_id': 1}}, {'$limit': 5}]
                results = datacorona.aggregate(pipeline=pipe)
                for result in results:
                    pprint.pprint(result)
            
        elif choice == 4:
            pipe = [{'$match': {'$and': [{'latest.deaths': {'$gte': 100}}, {'latest.recovered': {'$lt': 100}}]}}, {'$group' : {'_id':'$country', 'number_of_provinces':{'$sum': 1}}}, {'$sort' : {'_id': 1}}]
            results = datacorona.aggregate(pipeline=pipe)
            for result in results:
                pprint.pprint(result)

        elif choice == 5:
            pipe = [{'$group': {'_id': 'null', 'total_unresolved': {'$sum': {'$add': [{'$subtract': ['$latest.deaths', '$latest.recovered']}, '$latest.confirmed']}}}},{'$project': {'_id': 0}}]
            results = datacorona.aggregate(pipeline=pipe)
            for result in results:
                pprint.pprint(result)

        print('\nRun again or Exit ?')
        print('1. Run again !')
        print('2. Exit')
        choice = int(input())
        while(choice != 1 and choice !=2):
            print('Invalid input, please choose 1 or 2')
            choice = int(input())
        if choice ==1:
            print('Still running...')
        else: 
            print('Stopping...')
            break
    
    client.close() 

def update_database(datacorona, json_list):

    # New data from the stream API
    r = requests.get('https://coronavirus-tracker-api.herokuapp.com/v2/locations').text
    locations = json.loads(r)['locations']

    new_update = locations[-1]['last_updated'][:-12] # Check last update of the new data
    old_update = json_list[-1]['last_updated'][:-12] # Check last update of the old data     

    # Import the old data in a new list
    with open('coronadata.json') as f:
        mylist = json.load(f)
    
    # If there are some updates (<=> dates of updates from old and new data aren't the same)
    if new_update != old_update:

        # Delete old elements in the database
        datacorona.delete_many({})

        for location in locations: 

            # Insert new elements in the database
            datacorona.insert_one(location)  

            # Add new elements in the historic (json file)
            key_to_remove = '_id'
            location.pop(key_to_remove, None)
            mylist.append(location)
            

        with open('coronadata.json', 'w') as file:
            json.dump(mylist, file, indent=4)

        print('The data from:', locations[-1]['last_updated'][:-11], 'has been successfully updated')

    else:
        print('Latest updates from:', json_list[-1]['last_updated'][:-11], 'have already been installed')

    return datacorona, mylist

main()
