

def mergeTwoLists(list1, list2):

    merged_list = []

    while list1 and list2:
        if list1[0] <= list2[0]:
            merged_list.append(list1.pop(0))
        else:
            merged_list.append(list2.pop(0))

    return merged_list


list1 = [1,2,4]
list2 = [1,3,4]

print(mergeTwoLists(list1,list2))
