def two_sum(nums, target):
    """
    Finds two indices of numbers in the array that add up to the target using a nested loop.

    :param nums: List[int] - List of integers
    :param target: int - Target sum
    :return: List[int] - Indices of the two numbers
    """
    #print(len(nums))
    for i in range(len(nums)): #i=0 >> i++ =0+1 = 1 >> i++ = 1+1= 2
        print("index of i " , i)
        print("outer loop", nums[i])
        for j in range(i + 1, len(nums)): #j= 2 +1 = 3
            print("index of j", j)
            #[11, 12, 7, 2]
            print("inner loop", nums[j])
            if nums[i] + nums[j] == target:
                return [i, j]


# Examples
nums1 = [11, 12, 7, 2]
target1 = 9
print(two_sum(nums1, target1))  # Output: [0, 1]

