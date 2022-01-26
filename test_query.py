import boto3
import pprint
import time
import plotly.graph_objects as go
def query_table(dynamodb=None):
    dynamodb = boto3.resource('dynamodb',region_name='eu-west-2',aws_access_key_id='',aws_secret_access_key='')
    table = dynamodb.Table('skills_in_demand')

    #  APPEND ALL SKILLS TO A LIST THEN QUERY THAT LIST AND USE A COUNTER

    list2 = []
    lastEvaluatedKey = None
    items = [] # Result Array
    count = 0
    while True:
        if lastEvaluatedKey == None:
            response = table.scan() # This only runs the first time - provide no ExclusiveStartKey initially
        else:
            time.sleep(10)
            response = table.scan(
            ExclusiveStartKey=lastEvaluatedKey # In subsequent calls, provide the ExclusiveStartKey
        )

        items.extend(response['Items']) # Appending to our result set list
        

        # Set our lastEvlauatedKey to the value for next operation,
        # else, there's no more results and we can exit
        if 'LastEvaluatedKey' in response:
            lastEvaluatedKey = response['LastEvaluatedKey']
        else:
            break
    for dictionary in items:
        for key, value in dictionary.items():
            if key == 'SKILLS':
                list2.append(value)
    
    n_rows = []
    first_count = 0
    n_valid_rows =[]
    second_count = 0
    third_count=0
    n_invalid_rows =[]

    # COUNT ALL ROWS
    for x in list2:
        first_count = first_count + 1
    print(f'ROWS: {first_count}')

    # COUNT ALL VALID ROWS
    for x in list2:
        if isinstance(x, str):
            second_count = second_count +1
            n_valid_rows.append(x)
       
    print(f'VALID ROWS: {second_count}')

    # COUNT ALL INVALID ROWS
    for x in list2:
        if not isinstance(x, str):
            third_count = third_count +1
            n_invalid_rows.append(x)
       
    print(f'INVALID ROWS: {third_count}')

    new_our_skills = []
    
    # SEPARATE STRINGS WITH MULTIPLE VALUES:
    for x in n_valid_rows:
        new_our_skills.append(x.split(' '))
    
    # MUST REMOVE SQUARE BRACKETS
    query_list = []
    for x in new_our_skills:
        query_list.extend(x)

    dictionary = {}
    # COMPARE AND COUNT:
    for skill in set(query_list):
        n = query_list.count(skill)
        dictionary[skill] = n
    
    # top 10 skills:
    keys = sorted(dictionary, key=dictionary.get, reverse=True)[:20]

    # create new dictionary with only top 10:
    values = []
    for skills in keys:
        for k, v in dictionary.items():
            if k == skills:
                values.append(v)
    
    keys.remove('cli')
    keys.remove('lan')
    keys.remove('.net')
    keys.remove('learning')
    keys.remove('machine')
    keys.remove('data')
    keys.remove('rest')
    values.pop(0)
    values.pop(1)
    values.pop(2)
    values.pop(11)
    values.pop(12)
    values.pop(14)
    values.pop(4)
    keys= keys[:10]
    values= values[:10]
    pprint.pprint(keys)
    pprint.pprint(values)

    # create graph
    fig = go.Figure(data=[go.Pie(labels=keys, values=values)])
    fig.show()


if __name__ == '__main__':
    query = query_table() 