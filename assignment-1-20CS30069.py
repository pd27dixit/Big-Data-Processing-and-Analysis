# python3 assignment-1-20CS30069.py bdp-assign-1-data.txt query.txt 4 3
import sys
import heapq
from threading import Thread
import numpy as np
from numpy.linalg import norm

# Define a class for a Min Heap with a fixed size to store the top k elements
class MinHeap_K():
    def __init__(self, top_n):
        # Initialize the heap as an empty list
        self.h = []
        # Set the maximum length of the heap
        self.length = top_n
        # Heapify the list to maintain the heap property
        heapq.heapify(self.h)

    def add(self, element):
        # Add an element to the heap while maintaining the heap property
        if len(self.h) < self.length:
            heapq.heappush(self.h, element)
        else:
            heapq.heappushpop(self.h, element)
        # Re-heapify the list
        heapq.heapify(self.h)
        
    def getTop(self):
        return heapq.nsmallest(self.length, self.h)
    
    def sort(self):
        # Sort the heap in descending order (reverse=True)
        self.h.sort(reverse=True)

# Initialize an empty list to store cosine similarity scores
cosine_scores = []

# Function to calculate cosine similarity between two vectors
def cosine_similarity(vec1, vec2):
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = norm(vec1)
    norm_vec2 = norm(vec2)
    
    # Calculate the cosine similarity
    similarity = dot_product / (norm_vec1 * norm_vec2)
    return similarity

# def process_data(data, query_vectors, k, thread_id, priority_queue, start, end):
#     for i in range(start, end):
#         data_item = data[i]
#         data_id, data_vector = data_item[0], data_item[1]
#         for query_vector in query_vectors:
#             cosine_score = cosine_similarity(data_vector, query_vector)
#             priority_queue.push((data_id, cosine_score), cosine_score)
#             print(f"Thread {thread_id} - ID: {data_id}, Cosine Similarity: {cosine_score}")
#             cosine_scores.append(cosine_score)

# Function to process data in parallel using threads
def process_data(data, query_vectors, k, thread_id, priority_queue, start, end):
    for i in range(start, end):
        data_item = data[i]
        data_id, data_vector = data_item[0], data_item[1]
        for query_vector in query_vectors:
            # Calculate cosine similarity for each query vector
            cosine_score = cosine_similarity(data_vector, query_vector)
            # Using the negative cosine score as priority for MinHeap_K
            priority_queue.add((cosine_score, data_id))
            # Uncomment the next line to print individual similarity scores
            # print(f"Thread {thread_id} - ID: {data_id}, Cosine Similarity: {cosine_score}")
            cosine_scores.append(cosine_score)

# Function to read data from a file and store it in a list of ID-vector pairs
def read_data_from_file(file_path):
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.split()
            id_number = int(parts[0])
            vector = [float(x) for x in parts[1:]]
            data.append([id_number, vector])
    return data

# Function to read query vectors from a file
def read_query_vector(query_file):
    query_vectors = []
    with open(query_file, 'r') as file:
        for line in file:
            vector = [float(x) for x in line.split()]
            query_vectors.append(vector)
    return query_vectors

# Function to print the final result of the most similar k items
def print_result(final_result):
    print("Final Result of the most similar k items:")
    for item in final_result:
        print(f"ID: {item[0]}, Score: {item[1]}")

# Main function
def main():
    if len(sys.argv) != 5:
        print("Give proper number of command line arguments")
        sys.exit(1)

    # Extract command line arguments
    data_file = sys.argv[1]
    query_file = sys.argv[2]
    threads_count = int(sys.argv[3])
    k = int(sys.argv[4])

    # Read data from the input files
    data = read_data_from_file(data_file)
    query_vectors = read_query_vector(query_file)
    
    #threads_count = 4
    #k = 3

    # Create a MinHeap_K priority queue for each thread
    priority_queues = [MinHeap_K(k) for _ in range(threads_count)]

    # Create and start threads to process data in parallel
    threads = []
    size = len(data) // threads_count
    for i in range(threads_count):
        start = i * size
        end = (i + 1) * size if i != threads_count - 1 else len(data)
        thread = Thread(target=process_data, args=(data, query_vectors, k, i, priority_queues[i], start, end))
        threads.append(thread)
        thread.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()
        
    for i, pq in enumerate(priority_queues):
        heapq.heapify(pq.h)
        pq.sort()
        # print(f"Priority Queue {i + 1} contents: {pq.h}")
    
    # Sort and merge individual priority queues into a final priority queue
    final_priority_queue = MinHeap_K(k)
    for i, pq in enumerate(priority_queues):
        # Sort the individual priority queues in descending order
        pq.sort()
        # Merge the individual priority queues into the final priority queue
        for item in pq.h:
            final_priority_queue.add(item)

    # Print the final result of the most similar k items
    final_priority_queue.sort()
    print("\nFinal Result of the most similar k items:")
    for rank, (score, d_id) in enumerate(final_priority_queue.h):
        print(f"ID = {d_id}, Score = {score}")

    # Create a final priority queue incorporating all results
    # final_priority_queue = MinHeap_K(k)
    # for pq in priority_queues:
    #     final_priority_queue.queue.extend(pq.queue)
    #     heapq.heapify(final_priority_queue.queue)

    # # Get the first k smallest entries based on index-1
    # final_result = [(-heapq.heappop(final_priority_queue.queue)[0], heapq.heappop(final_priority_queue.queue)[1]) for _ in range(k)]

    # # Print the final result
    # print_result(final_result)

    # Sort the global array of cosine scores
    sorted_cosine_scores = sorted(cosine_scores, reverse=True)
    #print(sorted_cosine_scores[slice(20)])

if __name__ == "__main__":
    main()

