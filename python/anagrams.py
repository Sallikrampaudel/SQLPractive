"""
Group Anagrams Using a Dictionary
Write a Python function to group anagrams from a list of words using a dictionary.
Input: ['eat', 'tea', 'tan', 'ate', 'nat', 'bat']
Output: [['eat', 'tea', 'ate'], ['tan', 'nat'], ['bat']]
"""

def anagrams(list):
    newlist = {}
    for x in list:
        sorted_word = ''.join(sorted(x))
        #print(sorted_word)

        if sorted_word  in newlist:
            newlist[sorted_word].append(x)

        else:
            newlist[sorted_word]=[x]

    print(newlist.values())

if __name__ == '__main__' :
    input= ['eat', 'tea', 'tan', 'ate', 'nat', 'bat']
    anagrams(input)
