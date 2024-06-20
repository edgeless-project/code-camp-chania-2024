## General Approach
1) Create a scoring system for the nodes based on their hardware (cpu power, cores, frequency, memory)
2) Calculate the percentage and then the number of jobs that each node should handle at most (max jobs) based on their score
3) Based on the max jobs find an overworking node (current jobs > max jobs) and an underworking node (current jobs < max jobs)
4) Migrate a job from the overworking node to the underworking node
5) Repeat

### Pros
1) The scoring system can be easily modified by changing the corresponding line
2) The job distribution logic can also be easily modified
3) More complicated logic can easily be added (e.g. Migrate specific or more jobs at once, prioritize which nodes to unburden first based on their %usage, take into account how long a job has been running to avoid losing data, etc)

### Cons
1) Need to recalculate all the data on each iteration
