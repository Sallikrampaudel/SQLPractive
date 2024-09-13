"""
Invert a Dictionary
Write a Python function to invert a dictionary (swap keys and values).
Input: {'a': 1, 'b': 2, 'c': 3}
Output: {1: 'a', 2: 'b', 3: 'c'}
"""

def invert_dict(dict):
    inverted = {}
    for key, value in dict.items():
        inverted[value] = key
    print(inverted)


if __name__ == '__main__' :
    dict1= {'a': 1, 'b': 2, 'd': 3, 'c': 4}
    dict2={'b': 3, 'c': 4}
    invert_dict(dict1)
