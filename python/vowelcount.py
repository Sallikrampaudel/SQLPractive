"""
Problem Statement:
Question: Write a Python function to count the number of vowels (a, e, i, o, u) in a given string.
Input: "Hello World"
Output: 3
"""
#solution
def vowel(string):
    vowels=['a','e','i','o','u']
    count=0
    for x in string:
        if x in vowels:
            count+=1

    print(count)
if __name__ == '__main__' :
    vowel("Sample TEXT")
