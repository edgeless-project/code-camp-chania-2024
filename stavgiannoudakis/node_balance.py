'''
@author Stavgiannoudakis Georgios
'''

import json
import redis

def calculate_weight(node_id):
    return  nodes[node_id]['clock_freq_cpu'] / len(nodes[node_id]['instances'])
    

def find_max_min_weights():
    max_weight = -float('inf')
    min_weight = float('inf')
    max_node_id = None
    min_node_id = None
    
    for node_id, node in nodes.items():
        if node['weight'] > max_weight:
            max_weight = node['weight']
            max_node_id = node_id
        if node['weight'] < min_weight:
            min_weight = node['weight']
            min_node_id = node_id
            
    return max_node_id, min_node_id


    
redis_client = redis.Redis(host='127.0.0.1', port=32771)

# init
nodes = {}
cursor = '0'
all_keys = []

while cursor != 0:
    cursor, keys = redis_client.scan(cursor=cursor, match='node:capabilities:*')
    all_keys.extend(keys)

for key in all_keys:
    key_str = key.decode("utf-8")
    value = redis_client.get(key)
    
    if value:
        value_str = value.decode("utf-8")
        json_value = json.loads(value_str)
        node_id = key_str.split(':')[2]
        if json_value.get('runtimes'):  # Check if runtimes is not empty
            nodes[node_id] = {
                'num_cpus': json_value.get('num_cpus'),
                'num_cores': json_value.get('num_cores'),
                'clock_freq_cpu': json_value.get('clock_freq_cpu'),
                'runtimes': json_value.get('runtimes'),
                'instances' : [],
                'weight'    : 0
            }

# init
instance_data = {}
cursor = '0'
all_keys = []

while cursor != 0:
    cursor, keys = redis_client.scan(cursor=cursor, match='instance:*')
    all_keys.extend(keys)

for key in all_keys:
    key_str = key.decode("utf-8")
    value = redis_client.get(key)
    
    if value:
        value_str = value.decode("utf-8")  
        json_value = json.loads(value_str)
        instance_id = key_str.split(':')[1]
        node_id = json_value["Function"][1][0].split(':')[1].split(',')[0].strip()
        instance_data[instance_id] = node_id 
        
        nodes[node_id]['instances'].append(instance_id)


print("data stored")    #debug

#calculate all the weights
for node_id, node in nodes.items():
    node['weight'] = calculate_weight(node_id)

    
while True:
    print("looping")    #debug
    max_node_id, min_node_id = find_max_min_weights()

    max_weight = nodes[max_node_id]['weight']
    min_weight = nodes[min_node_id]['weight']

    if max_weight - min_weight < 20:   # smallest number without infinite loop
        break                           

    # move instance
    instance_to_move = nodes[min_node_id]['instances'].pop(0)
    nodes[max_node_id]['instances'].append(instance_to_move)
    
    # push changes 
    #redis_client.set(f"intent:migrate:{instance_to_move}", f"{min_node_id}")
    #redis_client.lpush("intents", f"intent:migrate:{instance_to_move}")
    
    # Recalculate weights after the move
    nodes[max_node_id]['weight'] = calculate_weight(max_node_id)
    nodes[min_node_id]['weight'] = calculate_weight(min_node_id)



for node_id, node in nodes.items():
    print(f"node id: {node_id}")
    print(f"  cpus: {node['num_cpus']}")
    print(f"  Cores: {node['num_cores']}")
    print(f"  clock_freq_cpu: {node['clock_freq_cpu']}")
    print(f"  weight: {node['weight']}")
    num_jobs = len(nodes[node_id]['instances'])
    print(f'  num ofjobs : {num_jobs}')
    print()
