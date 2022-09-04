
f = open('log', "r")

total = 0
for l in f:
    if "exit status 1" in l:
        total +=1

print("total failures: ", total)

