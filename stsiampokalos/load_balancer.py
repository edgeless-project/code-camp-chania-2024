import redis
import json
import time
import random
from collections import Counter

PORT = 32773
HOST = "localhost"
INTERVAL = 2


def main():
	global client
	client = redis.Redis(host=HOST, port=PORT)

	load_balancer()


def load_balancer():

	while True:
		# Get priority list based on scores
		plist = get_nodes_scores()
		
		# Get instance/node pairs
		pairs = get_instance_node_pairs()

		# Get number of max jobs that each node can handle based on score
		max_jobs = distribute_jobs(plist)
		
		# Get current number of jobs per node
		jpn = count_jobs_per_node(pairs)

		# Shuffler used for testing
		# shuffler(pairs)
		
		# Find the first overworking node
		onode = find_overworking_node(jpn, max_jobs)
		# Find the first underworking node
		unode = find_underworking_node(jpn, max_jobs)

		if onode != None and unode != None:
			print(f"[!] Overworked node {onode} | current jobs {jpn[onode]} | max jobs {max_jobs[onode]}")
			print(f"[!] Underworked node {unode} | current jobs {jpn[unode]} | max jobs {max_jobs[unode]}")

			# Get a job to migrate from the overworked node
			mjob = get_node_job(onode, pairs)

			print(f"[*] Migrating job {mjob} from node {onode} to node {unode}\n")
			migrate(mjob, unode)
		else:
			print("[+] Load is balanced\n")

		# Run interval
		time.sleep(INTERVAL)


def get_nodes_scores():
	i = 0
	priority_list = {}

	for node in client.keys('node:capabilities:*'):
		node = node.decode()
		node_id = node.split("'")[0].split(":")[-1]
		data = json.loads(client.get(node))

		if data["runtimes"]:
			cpus = data["num_cpus"]
			cores = data["num_cores"]
			freq = data["clock_freq_cpu"]
			mem = data["mem_size"]
			score = cpus * cores * freq * mem # This is the equation for the scoring system which can be easily modified
			priority_list[node_id] = score
		i += 1

	priority_list = dict(sorted(priority_list.items(), key=lambda item: item[1], reverse=True))

	return priority_list


def get_instance_node_pairs():
	i = 0
	pairs = {}
	for instance in client.keys('instance:*'):
		instance = instance.decode()
		data = json.loads(client.get(f'{instance}').decode())
		instance = instance.split(":")[-1]
		node_id = data['Function'][1][0].split(" ")[1][:-1]
		pairs[instance] = node_id
		i += 1
	return pairs


def distribute_jobs(plist):
	total_score = 0
	num_of_jobs = len(client.keys('instance:*'))

	for node in plist:
		total_score += plist[node]
	
	max_jobs = {}
	for node in plist:
		score = plist[node]
		percentage = (score * 100) / total_score
		max_jobs[node] = round((percentage / 100) * num_of_jobs)

	return max_jobs


def count_jobs_per_node(pairs):
	values = sorted(list(pairs.values()))
	occurrences = Counter(values)
	
	jobs_per_node = dict(sorted(occurrences.items(), key=lambda item: item[1], reverse=True))
	return jobs_per_node


def find_overworking_node(jpn, max_jobs):
	for node_id in jpn:
		if jpn[node_id] > max_jobs[node_id]:
			return node_id

	return None


def find_underworking_node(jpn, max_jobs):
	for node_id in jpn:
		if jpn[node_id] < max_jobs[node_id]:
			return node_id

	return None


def get_node_job(node, pairs):
	for job in pairs:
		if pairs[job] == node:
			return job


def migrate(job, node):
	client.set(f'intent:migrate:{job}', f'{node}')
	client.lpush('intents', f'intent:migrate:{job}')


def shuffler(pairs):
	nodes = list(pairs.values())
	for job in pairs:
		migrate(job, random.choice(nodes))
	exit()


if __name__ == "__main__":
	main()
