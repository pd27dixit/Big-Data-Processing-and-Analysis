# python3 a2.py out3.txt 1024 5
# python3 a3.py out3.txt 1000 379

import re
import heapq
import sys
from collections import defaultdict
from itertools import groupby
from collections import Counter
import psutil
import os
import math
import ast

# Global buffer variable
global_buffer = None


def get_ram_usage():
    # Get RAM usage information
    ram_info = psutil.virtual_memory()

    # Get RAM usage in bytes
    used_ram = ram_info.used
    total_ram = ram_info.total

    # Convert bytes to megabytes for better readability
    used_ram_mb = used_ram / (1024 ** 2)
    total_ram_mb = total_ram / (1024 ** 2)

    return used_ram_mb, total_ram_mb



def tokenize_and_filter(text):
    # words = re.findall(r'\b\w{3,}\b', text.lower())
    words = re.findall(r'\b\w{3,}\b', text, flags=re.UNICODE)
    
    # Remove 'â' if it appears at the end of a word
    words = [word.strip('â') for word in words]
    
    # Convert all words to lowercase
    words = [word.lower() for word in words]
    
    filtered_words = [word for word in words if word not in avoid_list]
    return filtered_words


def get_first_word(chunk):
    # Get the first non-empty line in the chunk
    for line in chunk:
        words = tokenize_and_filter(line)
        filtered_words = [word for word in words if len(re.findall(r'[a-zA-Z]', word)) >= 3]
        if filtered_words:
            return filtered_words[0]
    return None

def get_last_word(chunk):
    # Get the last non-empty line in the chunk
    for line in reversed(chunk):
        words = tokenize_and_filter(line)
        filtered_words = [word for word in words if len(re.findall(r'[a-zA-Z]', word)) >= 3]
        if filtered_words:
            return filtered_words[-1]
    return None

def print_bigrams_until_freq_one(f1_name, out5_name):
    with open(f1_name, 'r') as f1, open(out5_name, 'w') as out5:
        for line in f1:
            # Find the location of ':'
            colon_index = line.find(':')

            if colon_index != -1:
                # Extract bigram and frequency
                bigram = line[:colon_index].strip()
                freq = line[colon_index + 1:].strip()
                freq = int(freq)
                if freq == 1:
                    print("Found bigram with frequency 1. Stopping.")
                    return
                else:
                    out5.write(f"{bigram}: {freq}\n")
                
# Modify the map_function to print bigrams and frequencies
def map_function(chunk):
    global global_buffer, file_counter
    bi_grams_count = defaultdict(int)

    # Initialize a variable to keep track of the previous word
    prev_word = None

    for line in chunk:
        words = tokenize_and_filter(line)

        # Check if the previous word is not None
        if prev_word is not None:
            # Create a bigram with the previous word and the current first word
            first_word = words[0]
            if len(re.findall(r'[a-zA-Z]', prev_word)) >= 3 and len(re.findall(r'[a-zA-Z]', first_word)) >= 3:
                bi_grams_count[(prev_word, first_word)] += 1

        # Update the previous word for the next iteration
        prev_word = None if not words else words[-1]

        # Generate bi-grams with specific rules
        bi_grams = [(words[i], words[i + 1]) for i in range(len(words) - 1)
                    if len(re.findall(r'[a-zA-Z]', words[i])) >= 3 and
                    len(re.findall(r'[a-zA-Z]', words[i + 1])) >= 3]

        # Add the valid bi-grams to the count
        for bi_gram in bi_grams:
            bi_grams_count[bi_gram] += 1
            
    # Iterate through the items in bi_grams_count
    for bi_gram, count in bi_grams_count.items():
        # Create the data string
        data_string = f"{bi_gram}: {count}"
        add_to_global_buffer(data_string)

    

# def local_sort_and_shuffle(bi_grams_count):
#     # Local sort
#     sorted_bi_grams = sorted(bi_grams_count.items(), key=lambda x: x[1], reverse=True)

#     # Shuffle
#     return sorted_bi_grams


def local_sort_and_shuffle_file(file_path):
    # Read the content of the file
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.readlines()

    # Perform local sort and shuffle
    sorted_content = sorted(content, key=lambda x: int(x.split(':')[1]), reverse=True)

    # Write the sorted content back to the file
    with open(file_path, 'w', encoding='utf-8') as file:
        file.writelines(sorted_content)


def generate_bi_grams_for_chunk(chunk_boundaries):
    bi_grams_count = defaultdict(int)

    for i in range(0, len(chunk_boundaries) - 1, 2):
        word1 = chunk_boundaries[i]
        word2 = chunk_boundaries[i + 1]

        # Tokenize and filter
        words = tokenize_and_filter(f"{word1} {word2}")

        # Check if words has at least two elements
        if len(words) >= 2:
            # Check rules for each word in the bi-gram
            if len(re.findall(r'[a-zA-Z]', words[0])) >= 3 and len(re.findall(r'[a-zA-Z]', words[1])) >= 3:
                # Add the valid bi-gram to the count
                bi_grams_count[(words[0], words[1])] += 1

    # Continue with the rest of your code
    # return bi_grams_count

    # Iterate through the items in bi_grams_count
    for bi_gram, count in bi_grams_count.items():
        # Create the data string
        data_string = f"{bi_gram}: {count}"
        add_to_global_buffer(data_string)



def global_sort_and_select_top_k(sorted_partitions, k):
    # Merge sorted partitions
    merged_sorted_bi_grams = heapq.merge(*sorted_partitions, key=lambda x: x[0])

    # Use Counter to aggregate counts for duplicate key bi-grams
    aggregated_bi_grams = Counter()

    for key, group in groupby(merged_sorted_bi_grams, key=lambda x: x[0]):
        total_count = sum(item[1] for item in group)
        aggregated_bi_grams[key] += total_count

    # Convert Counter items to a list of tuples
    global_sorted_bi_grams = list(aggregated_bi_grams.items())

    # Global sort based on total count
    global_sorted_bi_grams = sorted(global_sorted_bi_grams, key=lambda x: x[1], reverse=True)

    # Select top k
    top_k_bi_grams = global_sorted_bi_grams[:k]

    return top_k_bi_grams

def write_top_k_to_file(top_k_bi_grams, output_file):
    with open(output_file, 'w', encoding='utf-8') as file:
        for bi_gram, count in top_k_bi_grams:
            file.write(f"{bi_gram}: {count}\n")
            
def get_available_ram():
    # Get available RAM in bytes
    available_ram = psutil.virtual_memory().available
    
    # Convert bytes to megabytes for better readability
    available_ram_mb = available_ram / (1024 ** 2)
    
    return available_ram_mb

def create_global_buffer(buffer_size_mb):
    global bytes_of_global_buffer, global_buffer
    # Convert buffer size from MB to bytes
    bytes_of_global_buffer = buffer_size_mb * 1024 * 1024
    # bytes_of_global_buffer = buffer_size_mb

    # Initialize global buffer as a list of strings
    global_buffer = []
    return global_buffer


def flush_global_buffer():
    global used_bytes_counter, remaining_space, file_counter, global_buffer
    # Check if the global buffer is not empty
    if global_buffer:
        file_path = f'file{file_counter}.txt'
        with open(file_path, 'w', encoding='utf-8') as file:
            for string_data in global_buffer:
                file.write(f"{string_data}\n")
        global_buffer.clear()
               
        file_counter += 1
        remaining_space = bytes_of_global_buffer
        used_bytes_counter = 0
        os.chmod(file_path, 0o777)  

           
def add_to_global_buffer(data):
    global used_bytes_counter
    global remaining_space
    global global_buffer
    
    space = ' '
    # Check if there is enough space to add the new data
    if len(data) <= remaining_space:
        # Add data to the buffer
        global_buffer.append(data)
        # global_buffer.append(space)
        remaining_space -= len(data)
        used_bytes_counter += len(data)
        # print(f"Added {len(data)} bytes to global_buffer")
        return True
    else:
        # return False  # Not enough space to add data to  Read input arguments
        print(f"Not enough space in buffer to add {data}")
        print(f"write to file{file_counter}")
        flush_global_buffer()
        return add_to_global_buffer(data)


def calculate_steps(n):
    if n % 2 == 0:
        steps = math.log2(n)
    else:
        steps = math.log2(n + 1)
    return steps
    
def aggregate_duplicate_bigrams(file_name):
    # Read content of file1
    with open(file_name, 'r') as file:
        content_lines = file.readlines()

    # Aggregate counts for duplicate bigrams
    aggregated_counts = {}
    for line in content_lines:
        # Use string parsing to extract bigram and count
        line = line.strip('()\n').replace("'", "").replace(" ", "")
        bigram_str, count_str = line.split(':')
        bigram = tuple(bigram_str.split(','))

        # Remove ')' from bigram[1]
        bigram = (bigram[0], bigram[1].rstrip(')'))

        count = int(count_str)

        # Update aggregated counts
        aggregated_counts[bigram] = aggregated_counts.get(bigram, 0) + count

    # Write aggregated counts back to file1
    with open(file_name, 'w') as file:
        for bigram, count in aggregated_counts.items():
            file.write(f"('{bigram[0]}', '{bigram[1]}'): {count}\n")

    print(f"Aggregated duplicate bigrams in {file_name}.")
    
    
def process_files(file_num1, file_num2):
    f1_name = f'file{file_num1}.txt'
    f2_name = f'file{file_num2}.txt'
    
    # Check if both files exist
    if os.path.exists(f1_name) and os.path.exists(f2_name):
        # Copy content of f2 to f1 line by line
        with open(f2_name, 'r') as file2:
            with open(f1_name, 'a') as file1:
                for line in file2:
                    file1.write(line)

        print(f"Remove {f2_name} file")
        os.remove(f2_name)

        # Aggregate duplicate bigrams in f1
        aggregate_duplicate_bigrams(f1_name)
    # else:
        # print(f"At least one of the files not found: {f1_name}, {f2_name}")

def process_remaining_files(file_numbers):
    while len(file_numbers) > 1:
        new_file_numbers = []

        # Process pairs of file numbers
        for i in range(0, len(file_numbers), 2):
            if i + 1 < len(file_numbers):
                process_files(file_numbers[i], file_numbers[i + 1])
                new_file_numbers.append(file_numbers[i])
        
        # If there's an odd number of files, the last one remains
        if len(file_numbers) % 2 == 1:
            new_file_numbers.append(file_numbers[-1])

        file_numbers = new_file_numbers

    return f'file{file_numbers[0]}.txt'
    
input_file = sys.argv[1]
buffer_size = int(sys.argv[2])
k = int(sys.argv[3])

# Check current RAM usage
#used_ram, total_ram = get_ram_usage()

#print(f"Used RAM: {used_ram:.2f} MB")
#print(f"Total RAM: {total_ram:.2f} MB")

# Check available RAM
available_ram = get_available_ram()
    
print(f"Available RAM: {available_ram:.2f} MB")

# Calculate buffer size as 80% of available RAM
adjusted_buffer_size = int(available_ram * 0.80)

# Check if buffer_size is greater than 80% of available RAM
if buffer_size > adjusted_buffer_size:
    print("Warning: Buffer size is greater than 80% of available RAM. Adjusting buffer size.")
    buffer_size = adjusted_buffer_size

print(f"Buffer Size: {buffer_size} MB")

# global global_buffer
global_buffer = create_global_buffer(buffer_size)
bytes_of_global_buffer = buffer_size*1024*1024
# bytes_of_global_buffer = buffer_size
remaining_space = bytes_of_global_buffer
used_bytes_counter = 0
file_counter = 1

print(f"Buffer Size: {bytes_of_global_buffer} Bytes")


# element1 = b'abc'
# element2 = b'def'

# add_to_global_buffer(element1)

# add_to_global_buffer(element2)
# # Print each string in the list
# for string_data in global_buffer:
#     print(string_data)
# # Print the current contents of the global buffer
# print("used_byte_counter: ", used_bytes_counter)
# add_to_global_buffer(element2)
    
# for string_data in global_buffer:
#     print(string_data)
# # Print the current contents of the global buffer
# print("used_byte_counter: ", used_bytes_counter)

# sys.exit()

# Read the avoid list
with open('word-list.txt.txt', 'r', encoding='utf-8') as file:
    # avoid_list = set(word.strip('"') for word in file.read().split())
    avoid_list = set(word.strip('",') for word in file.read().split())

#Process the file in chunks
# flag=0
chunk_boundaries = []  # Define chunk_boundaries outside the loop
with open(input_file, 'r', encoding='utf-8') as file:
    sorted_partitions = []

    while True:
        chunk = file.readlines(bytes_of_global_buffer)
        if not chunk:
            break
        
        # Get the first and last word of the chunk
        # print("chunk is ")
        # print(chunk)
        first_word = get_first_word(chunk)
        last_word = get_last_word(chunk)

        # Add the first and last word to the list
        chunk_boundaries.append(first_word)
        chunk_boundaries.append(last_word)
        
        
        print("First word of the chunk:", first_word)
        print("Last word of the chunk:", last_word)
        print("\n")
        
        # Apply map function to each chunk
        # print(chunk)
        # break
        map_function(chunk)
        # sys.exit()

        # Perform local sort and shuffle
        # sorted_bi_grams = local_sort_and_shuffle(bi_grams_count)
        # print("\n")
        # # for entry in sorted_bi_grams:
        # #     print(entry)
        
        # print("\n\n")    
        # # break
        # # if(flag==0):
        #     # print(sorted_bi_grams)
        #     # flag=1
        # # print(type(sorted_bi_grams))
        # sorted_partitions.append(sorted_bi_grams)

    
# sys.exit()
# Delete the first and last entries
if chunk_boundaries:
    del chunk_boundaries[0]  # Delete the first entry
if chunk_boundaries:
    del chunk_boundaries[-1]  # Delete the last entry
    
# Now you can access the modified chunk_boundaries
print("Modified Chunk Boundaries:", chunk_boundaries)
# Continue with the rest of the steps for global sort, selecting top k, and outputting results
print("\n")
# print((sorted_partitions))

# Generate bi-grams for consecutive pairs of words
print("BG for chunk boundaries\n")
generate_bi_grams_for_chunk(chunk_boundaries)


print("final buffer empty\n")
if global_buffer:
        file_path = f'file{file_counter}.txt'
        with open(file_path, 'w', encoding='utf-8') as file:
            for string_data in global_buffer:
                file.write(f"{string_data}\n")
        global_buffer.clear()
        file_counter += 1
        remaining_space = bytes_of_global_buffer
        used_bytes_counter = 0
        os.chmod(file_path, 0o777)

fc=1
while fc < file_counter:
    file_path = f'file{fc}.txt'
    # print("called\n")
    local_sort_and_shuffle_file(file_path)
    fc += 1





number_of_steps = calculate_steps(file_counter)

# for step in range(1, file_counter,2):
# Generate file names for the current step
#sys.exit()


# step = 1

# while True:
#     f1_name = f'file{step}.txt'
#     f2_name = f'file{step + 1}.txt'
    
#     # Check if both files exist
#     if os.path.exists(f1_name) and os.path.exists(f2_name):
#         # Copy content of f2 to f1 line by line
#         with open(f2_name, 'r') as file2:
#             with open(f1_name, 'a') as file1:
#                 for line in file2:
#                     file1.write(line)

#         print(f"Remove {f2_name} file")
#         os.remove(f2_name)

#         # Aggregate duplicate bigrams in f1
#         aggregate_duplicate_bigrams(f1_name)

#         step += 
#     else:
#         print(f"Only one file remaining: {f1_name}")
#         break



# Example usage:
# file_counter = 13  # Set to your desired value
file_numbers = list(range(1, file_counter + 1))
f1_name = process_remaining_files(file_numbers)

print(f"The last remaining file is: {f1_name}")
    

aggregate_duplicate_bigrams(f1_name)
local_sort_and_shuffle_file(f1_name)

print("\n\n\n\n\n\n\n\n\n\n\n\n")

print("Final Output:")
print("\n")

with open(f1_name, 'r') as file1:
    for _ in range(k):
        line = file1.readline()
        if not line:
            break
        print(line.strip())


    
sys.exit()

# Example usage
print_bigrams_until_freq_one(f1_name, 'out5.txt')

sys.exit()


# import shutil
 
# with open(f1_name, 'a') as f:
#     shutil.copyfileobj(f2_name, f)
# 

# sys.exit()



# Perform local sort and shuffle
sorted_bi_grams = local_sort_and_shuffle(bi_grams_count)
# if(flag==0):
# print(sorted_bi_grams)
# flag=1
# print(type(sorted_bi_grams))
sorted_partitions.append(sorted_bi_grams)


top_k_bi_grams = global_sort_and_select_top_k(sorted_partitions, k)

# # Output the result
# for bi_gram, count in top_k_bi_grams:
#     print(f"{bi_gram}: {count}")


# Output the result
output_file_path = 'out5.txt'
write_top_k_to_file(top_k_bi_grams, output_file_path)
