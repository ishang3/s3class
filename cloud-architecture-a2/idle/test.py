import json
temp = ['ishan','gupta','yes']

while True:
    with open('./object_data_boom.json', mode='w', encoding='utf-8') as f:
        json.dump(temp,f)