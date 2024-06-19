import json
import redis


    
redis_client = redis.Redis(host='127.0.0.1', port=32771)

capabilities_data = {}
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
            capabilities_data[node_id] = {
                'num_cpus': json_value.get('num_cpus'),
                'num_cores': json_value.get('num_cores'),
                'clock_freq_cpu': json_value.get('clock_freq_cpu'),
                'runtimes': json_value.get('runtimes'),
                'instances' : [],
                'weight'    : 0
            }

instance_data = {}
cursor = '0'
all_keys = []

while cursor != 0:
    cursor, keys = redis_client.scan(cursor=cursor, match='instance:*')
    all_keys.extend(keys)

# Process each key-value pair for instance data
for key in all_keys:
    key_str = key.decode("utf-8")
    value = redis_client.get(key)
    
    if value:
        value_str = value.decode("utf-8")  
        json_value = json.loads(value_str)
        instance_id = key_str.split(':')[1]
        node_id = json_value["Function"][1][0].split(':')[1].split(',')[0].strip()  # Extract only node_id
        instance_data[instance_id] = node_id 
        
        capabilities_data[node_id]['instances'].append(instance_id)


print("data stored")
#calculate all the
for node_id, capabilities in capabilities_data.items():
    capabilities['weight'] = (capabilities['num_cpus']*capabilities['clock_freq_cpu']*capabilities['num_cores'])/len(capabilities_data[node_id]['instances'])
    

print("Capabilities Data:")
for node_id, capabilities in capabilities_data.items():
    print(f"Node ID: {node_id}")
    print(f"  CPUs: {capabilities['num_cpus']}")
    print(f"  Cores: {capabilities['num_cores']}")
    print(f"  Clock Frequency (CPU): {capabilities['clock_freq_cpu']}")
    print(f"  weight: {capabilities['weight']}")
    num_jobs = len(capabilities_data[node_id]['instances'])
    print(f'  num ofjobs : {num_jobs}')
    print(f"{capabilities['instances']}")
    
    print()
