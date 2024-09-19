"""
Find the First Non-Repeating Character
Write a Python function to find the first non-repeating character in a given string and return its index.
Input: "swiss"
Output: 1 (for 'w' in "swiss")
"""

def nonRepeatChar(s):
    char_count = {}

    for char in s:
        if char in char_count:
            char_count[char] += 1
        else:
            char_count[char] = 1

    for char in s:
        if char_count[char] == 1:
            return char
    return None

if __name__ == '__main__' :
    input="swiss"
    print(nonRepeatChar(input))