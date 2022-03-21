"""Takes in the paths of two directories and reads all dump files. Computes md5 hashes of the dumped pages and performs analysis"""
import os
import sys
import hashlib


def compute_hash(chunk):
    hash_obj = hashlib.sha1(chunk)
    hash = hash_obj.hexdigest()
    return hash


def get_statistics_subpage(hash_table):
    # print('Total hashes in hash table: ' + str(len(hash_table)))
    count = 0
    for key in hash_table:
        num_chunks = len(hash_table[key])
        count += num_chunks

    # print('Total chunks: ' + str(count))
    return count


def get_common_chunks(table1, total1, table2, total2):
    common_hashes = list(set(table1.keys()) & set(table2.keys()))
    # print('Total Common Hashes: ' + str(len(common_hashes)))

    # print('Breakdown:')
    tuples_count = {}
    count1 = 0
    count2 = 0
    common_pages1 = []
    common_pages2 = []
    for key in common_hashes:
        num_chunks1 = len(table1[key])
        num_chunks2 = len(table2[key])
        count1 += num_chunks1
        count2 += num_chunks2
        # if (num_chunks1 > 100 and num_chunks2 > 100):
        #     print(key, num_chunks1, num_chunks2)
        common_pages1.extend(table1[key])
        common_pages2.extend(table2[key])
        number_tuple = '(' + str(num_chunks1) + ' ' + str(num_chunks2) + ')'
        if number_tuple in tuples_count:
            tuples_count[number_tuple] += 1
        else:
            tuples_count[number_tuple] = 1

    # print('Chunks in Table 1 that are common: ' + str(count1) + ', ' +
    #       str(float(count1) / total1))
    # print('Chunks in Table 2 that are common: ' + str(count2) + ', ' +
    #       str(float(count2) / total2))
    percent1 = float(count1) / total1
    percent2 = float(count2) / total2
    return percent1, percent2


def read_dumps(dir, num_hashes, chunk_size):
    table = {}
    page_id = 0

    for subdir, _, files in os.walk(dir):
        for file in files:
            zero_fp = 0
            if file[:5] == "pages":
                filename = os.path.join(subdir, file)
                num_pages = 0

                # Read the binary file
                fo = open(filename, "rb")
                mem = fo.read()
                if mem:
                    chunks_found = 0
                    for i in range(len(mem) - chunk_size + 1):
                        start = i
                        end = i + chunk_size
                        chunk = mem[start:end]

                        # Compute hash
                        hash = compute_hash(chunk)
                        # Insert into table
                        # if hash != "0b8bf9fc37ad802cefa6733ec62b09d5f43a1b75":
                        chunks_found += 1
                        if hash in table:
                            table[hash].append(page_id)
                        else:
                            table[hash] = [page_id]

                        if chunks_found == num_hashes:
                            break

    return table


def read_dumps_rabin(dir, num_hashes, chunk_size):
    table = {}
    period = 2 * chunk_size

    for subdir, _, files in os.walk(dir):
        for file in files:
            if file[:5] == "pages":
                filename = os.path.join(subdir, file)
                num_pages = 0

                # Read the binary file
                fo = open(filename, "rb")
                mem = fo.read()
                if mem:
                    chunks_found = 0
                    start = 0
                    while start < len(mem) - period + 1:
                        chunk = mem[start:start + chunk_size]
                        match_chunk = mem[start:start + period]

                        # Compute hash
                        hash = compute_hash(chunk)
                        # Insert into table
                        # if hash != "0b8bf9fc37ad802cefa6733ec62b09d5f43a1b75":
                        chunks_found += 1
                        if hash in table:
                            flag = False
                            for matching_chunk in table[hash]:
                                if matching_chunk == match_chunk:
                                    flag = True
                                    break
                            if not flag:
                                table[hash].append(match_chunk)
                        else:
                            table[hash] = [match_chunk]

                        if chunks_found == num_hashes:
                            break

                        start += period

    return table


def get_redundancy_rabin(dir, chunk_size, ref_table):
    period = 2 * chunk_size
    for subdir, _, files in os.walk(dir):
        for file in files:
            duplicate_bytes = 0
            total_bytes = 0
            if file[:5] == "pages":
                filename = os.path.join(subdir, file)

                # Read the binary file
                fo = open(filename, "rb")
                mem = fo.read()
                if mem:
                    total_bytes += len(mem)
                    start = 0
                    while start < len(mem) - period + 1:
                        chunk = mem[start:start + chunk_size]
                        match_chunk = mem[start:start + period]

                        # Compute hash
                        hash = compute_hash(chunk)
                        # Insert into table
                        # if hash != "0b8bf9fc37ad802cefa6733ec62b09d5f43a1b75":
                        if hash in ref_table:
                            max_length = 0
                            for matching_chunk in ref_table[hash]:
                                match_length = 0
                                if matching_chunk[:chunk_size] == chunk:
                                    # Check the remaining bytes
                                    match_length = chunk_size
                                    for i in range(period - chunk_size):
                                        if matching_chunk[chunk_size +
                                                          i] == match_chunk[
                                                              chunk_size + i]:
                                            match_length += 1
                                        else:
                                            break
                                if match_length > max_length:
                                    max_length = match_length
                            duplicate_bytes += max_length

                        start += period

    return float(duplicate_bytes) / total_bytes


def calc(name1, name2, chunk_size):
    table1 = read_dumps(name1, 5000000, chunk_size)
    table2 = read_dumps(name2, 5000000, chunk_size)

    count1 = get_statistics_subpage(table1)
    count2 = get_statistics_subpage(table2)
    p1, p2 = get_common_chunks(table1, count1, table2, count2)
    print(p1, p2)
    return p1, p2


def calc_rabin(name1, name2, chunk_size):
    table2 = read_dumps_rabin(name2, 5000000, chunk_size)
    p1 = get_redundancy_rabin(name1, chunk_size, table2)

    table1 = read_dumps_rabin(name1, 5000000, chunk_size)
    p2 = get_redundancy_rabin(name2, chunk_size, table1)

    print(p1, p2)
    return p1, p2
