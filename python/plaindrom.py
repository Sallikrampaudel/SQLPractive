from unicodedata import digit


def isPalindrome(x: int):
    if x < 0:
        return False

    temp = x
    reverse=0
    while(x>0):
        digit=x%10
        reverse=reverse*10+digit
        x=x//10

    print(reverse)

    if (temp==reverse):
        return True
    else:
        return False

x = 121
print(isPalindrome(x))