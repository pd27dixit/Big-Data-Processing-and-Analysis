
# def count_lines(filename):
#     try:
#         with open(filename, 'r') as file:
#             line_count = 0
#             for line in file:
#                 line_count += 1
#         return line_count
#     except FileNotFoundError:
#         print(f"Error: File '{filename}' not found.")
#         return -1

# # Example usage:
# filename = 'facebook_combined.txt'  # Replace 'example.txt' with your file path
# lines = count_lines(filename)
# if lines != -1:
#     print(f"Number of l
#     ........ines in '{filename}': {lines}")




# HOW TO RUN CODE

# cd /home/pd-priyanshi/Documents/Big_Data_Assgn/A3
# spark-submit /home/pd-priyanshi/Documents/Big_Data_Assgn/A3/assignment-3-20CS30069.py /home/pd-priyanshi/Documents/Big_Data_Assgn/A3/


# Import SparkSession from PySpark SQL and math.sqrt for square root calculation
from pyspark.sql import SparkSession
from math import sqrt

# Initialize Spark session
sc = SparkSession.builder \
    .appName("Heavy-Hitters_TriangleCounting") \
    .getOrCreate()

# Load graph data as an RDD of lines, split each line to extract nodes
graph_data = sc.read.text("facebook_combined.txt").rdd.map(lambda x: x[0].split())

# Preprocessing

# Create edges RDD with distinct edges
edges = graph_data.map(lambda x: (int(x[0]), int(x[1]))).distinct()

# Compute degrees for each node
degrees = edges.flatMap(lambda x: [(x[0], 1), (x[1], 1)]).reduceByKey(lambda a, b: a + b)

# Create an edge index (hash table) for efficient edge lookup
edge_index = edges.map(lambda x: ((x[0], x[1]), 1)).collectAsMap()

# Generate an adjacency list for each node
adjacency_list = edges.groupByKey().mapValues(list).collectAsMap()

# Count the number of edges
num_edges = edges.count()
# print("Number of edges:", num_edges)

# Print all nodes and their degrees
# print("\nNode Degrees:")
# for node, degree in degrees.collect():
#     print(f"Node {node}: Degree {degree}")

# Calculate the threshold for heavy hitter nodes
threshold = sqrt(num_edges)

# Identify heavy hitter nodes based on the threshold
heavy_hitters = degrees.filter(lambda x: x[1] >= threshold).map(lambda x: x[0]).collect()

# Print heavy hitter nodes
# print("\nHeavy Hitter Nodes:")
# print(heavy_hitters)

# Count heavy hitter triangles
heavy_hitter_triangles = 0
for i in range(len(heavy_hitters)):
    for j in range(i + 1, len(heavy_hitters)):
        for k in range(j + 1, len(heavy_hitters)):
            v1, v2, v3 = heavy_hitters[i], heavy_hitters[j], heavy_hitters[k]
            if edge_index.get((v1, v2)) and edge_index.get((v2, v3)) and edge_index.get((v1, v3)):
                heavy_hitter_triangles += 1

# Count other triangles involving non-heavy-hitter nodes
other_triangles = edges.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]) \
    .filter(lambda x: x[0] < x[1]) \
    .flatMap(lambda x: [(x[0], y) for y in adjacency_list.get(x[1], [])]) \
    .filter(lambda x: x[1] not in heavy_hitters) \
    .map(lambda x: ((x[0], x[1]), edge_index.get((x[0], x[1]), 0))) \
    .reduceByKey(lambda a, b: a + b) \
    .filter(lambda x: x[1] > 0) \
    .count()

# Print the final results 
print("\n\n\n\n\n")
print("No of heavy hitter nodes:", len(heavy_hitters))
print("No of triangles:", heavy_hitter_triangles + other_triangles)
print("\n\n\n\n\n")

# Stop Spark session
sc.stop()


# [0, 107, 1684, 1912, 3437]
# Number of edges: 88234
# No of heavy hitter nodes: 5
# No of triangles: 79672

