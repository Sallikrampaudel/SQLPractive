"""
Find Missing Number in an Array
Question: Given a list of integers from 1 to n with one number missing, find the missing number
"""
def missingNumber(list):
    print(list)
    n=len(list) +1

    expected_sum = n * (n + 1) // 2
    actual_sum = sum(list)

    missingNumber = expected_sum -actual_sum

    print(missingNumber)


def usingSet(arr):
    missing_value = set(range(arr[0], arr[-1] + 1)) - set(arr)
    print(missing_value)


if __name__ == '__main__' :
    list =[1,13,2,3,4,5,6,8,10,11,12]
    usingSet(list)