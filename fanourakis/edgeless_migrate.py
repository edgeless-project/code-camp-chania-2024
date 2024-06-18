'''
@author Fanourakis Nikos
'''

import redis
import json

def migrate():
    # Open connection
    r = redis.Redis(host='localhost', port=32773, decode_responses=True)
    # Find all node and function keys
    nodes_keys = r.scan_iter(match='node:capabilities:*')
    function_keys = r.scan_iter(match='instance:*')
    # print(nodes_keys)
    # print(function_keys)
    nodes_data = {}
    for key in nodes_keys:
        input_data = json.loads(r.get(key))
        # print(input_data)
        # Example: {"num_cpus":40,"model_name_cpu":"Intel(R) Xeon(R) Silver 4410T","clock_freq_cpu":1745.0,"num_cores":20,"mem_size":3760704,"labels":[],"is_tee_running":false,"has_tpm":false,"runtimes":["RUST_WASM"]}
        # keep only that with non-empty runtimes list
        if not input_data['runtimes']:
            continue
        # calculate power
        # run starts with 1 and not 0 because in the division later (calculation of the optimal node) we want to see the power that the function would take if assigned to this node.
        nodes_data[key.removeprefix('node:capabilities:')] = {
            'power': float(input_data['num_cpus']) * float(input_data['num_cores']) * float(input_data['clock_freq_cpu']),
            'run' : 1,
            'functions': []
        }
    # print(nodes_data)
    for key in function_keys:
        function_id = key.removeprefix('instance:')
        input_data = json.loads(r.get(key))
        # print(input_data)
        if 'Function' not in input_data:
            continue
        next = choose_next(nodes_data)
        nodes_data[next]['run'] += 1
        nodes_data[next]['functions'].append(function_id)
        # assign function id to next
        intent_function_id = f'intent:migrate:{function_id}'
        r.set(intent_function_id, next)
        # set intents
        r.lpush('intents', intent_function_id)
    # print(nodes_data) # the result
    pretty_print(nodes_data=nodes_data)

def choose_next(data):
    next = ""
    max = -1
    for key, val in data.items():
        var = val['power'] / val['run']
        if max < var:
            max = var
            next = key
    return next

def pretty_print(nodes_data):
    for node in nodes_data:
        print('---')
        print(f"Node: {node}")
        print(f"Power: {nodes_data[node]['power']}")
        print(f"Number of Functions: {nodes_data[node]['run'] - 1}")
        print(f"Functions: {nodes_data[node]['functions']}")

# Not in use now, but it could be used to sort nodes based on their power/capacity, in order to assign functions to the nodes faster when the first ones arrive in bunches.
# e.g. Receiving 3 functions simultaneously, the 1st function is assigned to the 1st node instantly, the 2nd function is assigned to either the same node or the 2nd in the list, whatever has the biggest capacity after the 1st assignment etc.
# After the whole bunch is assigned or after the length of the shorter list has reached the length of the list containing all the nodes sorted, we recalculate the power/capacity of each node and sort again and repeat the process.
# This way we do less comparisons than my implemented approach here. May seem useful when the orchestrator is dealing with a big number of functions and nodes.
# But I'm sure there are other data structures better for this job, like re-balancing trees etc.
def sort_by_capacity(data):
    return sorted(data.items(), key=lambda x: (-(get_capacity(x[1])), x[0]))

def get_capacity(data):
    return data['power'] / data['run']

def main():
    migrate()

if __name__ == "__main__":
    main()
