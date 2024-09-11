"""
Find the Intersection of Two Lists
Write a Python function to find the intersection of two lists.
Input: [1, 2, 3, 4], [3, 4, 5, 6]
Output: [3, 4]
"""

def intersection(list1, list2):
    intersectionList=[]
    for x in list1:
        if x in list2:
            intersectionList.append(x)
    print(intersectionList)


if __name__ == '__main__' :
    list1= [1, 2, 2, 3, 4]
    list2= [3, 4, 5, 6]
    intersection(list1, list2)
