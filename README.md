# code-camp-chania-2024

Code developed by the participants of the 1st EDGELESS Code Camp in Chania (Crete, Greece) on June 18, 2024

## Challenge: Load balancing delegated orchestrator

- Programming language: Python 
- Setup
  - An orchestration domain with 10 nodes deployed
  - Each node is assigned a random color as label and declares a random number of CPUs
  - A set of static workflows consisting of a single function

Task: _develop a delegated orchestrator that reads the orchestration domain status from Redis and produces a set of intents to balance as much as possible the load across nodes_

## Participants

Four different implementations of the task have been developed during the code camp, in about 3 hours:

- [Stavros Tsiampokalos](./stsiampokalos)
- [Fanourakis Nikos](./fanourakis)
- [Ioannis Christofilogiannis](./christofilogiannis)
- [Georgios Stavgiannoudakis](./stavgiannoudakis)

The delegated orchestrators developed have been tested with an EDGELESS cluster of 10 nodes with a WASM runtime, each running in a separate container, and an initial set of 100 workflows (each consisting of a single function).
