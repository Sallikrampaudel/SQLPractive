"""
Compress a String Using the Counts of Repeated Characters 
Write a Python function to perform basic string compression using the counts of repeated characters.
Input: "aabcccccaaa"
Output: "a2b1c5a3"
"""
def compress(str):
    copressed=[]
    count = 1
    for x in range(1, len(str)):
        if str[x] == str[x-1]:
            print(str[x], str[x-1])




if __name__ == '__main__' :
    input= "aabcccccaaa"
    compress(input)
