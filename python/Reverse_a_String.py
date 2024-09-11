"""
Reverse a String
Write a Python function to reverse a given string.
Input: "hello"
Output: "olleh"
"""
#Solution
def reverse_string(str):
     print(str[::-1])


if __name__ == '__main__' :
    str='Sample Text Is Here'
    reverse_string(str.lower())
