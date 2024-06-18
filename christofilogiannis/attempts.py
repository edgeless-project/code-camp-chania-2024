import json
import redis

redis_client = redis.Redis(host='127.0.0.1', port=32773)

# Fetch and process capabilities data
capabilities_data = {}
iterator = '0'
all_keys = []

# Redis scan of all the data
# https://stackoverflow.com/questions/22255589/get-all-keys-in-redis-database-with-python
while iterator != 0:
    cursor, keys = redis_client.scan(cursor=iterator, match='node:capabilities:*')
    all_keys.extend(keys)

# Get key value pairs from group of keys
for key in all_keys:
    key_str = key.decode("utf-8") # Decode bytes
    value = redis_client.get(key)
    if value:
        value_str = value.decode("utf-8")  # Decode bytes
        json_value = json.loads(value_str)
        node_id = key_str.split(':')[2]
        runtimes = json_value.get('runtimes', {})
        if runtimes:  # Only add if runtimes is not an empty dictionary, because we need 'RUST_WASM'
            capabilities_data[node_id] = {
                'num_cpus': json_value.get('num_cpus'),
                'num_cores': json_value.get('num_cores'),
                'clock_freq_cpu': json_value.get('clock_freq_cpu'),
                'runtimes': runtimes, # needed to check if its empty
            }


print("Sorted Node List by Clock Frequency:")
for node_id, clock_freq in sorted_node_list:
    print(f"Node ID: {node_id} - Clock Frequency (CPU): {clock_freq}")
print("Capabilities Data:")
# For debugging
for node_id, capabilities in capabilities_data.items():
    print(f"Node ID: {node_id}: Clock-{capabilities['num_cores']}, CPUs-{capabilities['num_cpus']}, Cores-{capabilities['num_cores']} ")
    print(f"  Runtimes: {capabilities['runtimes']}\n")

# Fetch and process instance data
instance_data = {}
cursor = '0'
all_keys = []

while iterator != 0:
    iterator, keys = redis_client.scan(cursor=iterator, match='instance:*')
    all_keys.extend(keys)

for key in all_keys:
    key_str = key.decode("utf-8")
    value = redis_client.get(key)

    if value:
        value_str = value.decode("utf-8")
        try:
            json_value = json.loads(value_str)
            instance_id = key_str.split(':')[1]
            node_id = json_value["Function"][1][0].split(':')[1].strip()
            instance_data[f"instance:{instance_id}"] = {
                "Function": [
                    [f"InstanceId(node_id: {node_id}, instance_id: {instance_id})"]
                ]
            }
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"Error processing key {key_str}: {str(e)}")

print("Instance Data:")
for instance_id, instance_details in instance_data.items():
    print(f"Instance ID: {instance_id}, Annotations: {instance_details['Function'][0]['annotations']}, Instance ID and Node ID: {instance_details['Function'][1][0]}")
    print()

# Combine data, may be useful for later
# combined_data = {
#     'capabilities': capabilities_data,
#     'instances': instance_data
# }

# Calculations for scheduler
# Will ignore the num_of_cpus and num_of_cores as everything has the same
# Sort node IDs by clock frequency
sorted_node_ids = sorted(capabilities_data.keys(), key=lambda node_id: capabilities_data[node_id]['clock_freq_cpu'], reverse=True)

# Store sorted node IDs in a list
sorted_node_list = [(node_id, capabilities_data[node_id]['clock_freq_cpu']) for node_id in sorted_node_ids]

# the number of nodes and instances for theoretical calculations
num_nodes = len(sorted_node_ids)
num_instances = len(instance_data)

# mean number of jobs per node
mean_jobs = num_instances / num_nodes if num_nodes > 0 else 0
print(num_nodes)
print(mean_jobs)
print(num_instances)

# After deciding migrations to make:
# migrations - scheduling
migrations = []

# Placeholder code
for source_id, destination_id in migrations:
    command_template = f"set intent:migrate:{source_id} {destination_id}" # or use redis commands instead if subprocess is not applicable
    run_command(command_template)
command_push = f"lpush intents *"
run_command(command_push)


