
def list_to_dict(list):
    counter = {}
    for word in list:
        if word in counter:
            counter[word] += 1
        else:
            counter[word] = 1
    return counter

def frequecy(dict):
    maxkey = dict["hello"]
    i=0
    for x in dict:
        if (dict[x] > i):
            i = dict[x]
            maxkey = x
    new_list = []
    for y in dict:
        if dict[maxkey]==dict[y]:
            dict[maxkey] = dict[y]
            new_list.append(y)

    return new_list

if __name__ == '__main__' :

    input_paragraph = "Hello world Hello world This world is full of surprises Surprises are everywhere; surprises are fun"
    words=input_paragraph.lower().split()
    dist = list_to_dict(words)
    max=frequecy(dist)
    print(dist)
    print("Most frequent word(s):", max)
