"""
Remove Duplicates from a List
Write a Python function to remove duplicates from a list while preserving the order.
Input: [1, 2, 2, 3, 4, 4, 5]
Output: [1, 2, 3, 4, 5]
"""

#Solution
def remove_duplicates(list):
    new_list=[]
    for x in list:
        if x not in new_list:
            new_list.append(x)
    print(new_list)

if __name__ == '__main__' :
    Input= [1, 2, 2, 3, 4, 4, 5]
    remove_duplicates(Input)
