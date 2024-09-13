"""
Write a Python function to merge two dictionaries.
Input: {'a': 1, 'b': 2}, {'b': 3, 'c': 4}
Output: {'a': 1, 'b': 3, 'c': 4}
"""

def merge_dict(dict1, dict2):
    merge = {}
    for x in dict1:
        merge[x] = dict1[x]
    for x in dict2:
        merge[x] = dict2[x]

    print(merge)

if __name__ == '__main__' :
    dict1= {'a': 1, 'b': 2}
    dict2={'b': 3, 'c': 4}
    merge_dict(dict1, dict2)
