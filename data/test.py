f = open("test.csv", "r")
for line in f:
    if not len(line.split(",")) == 99:
        print len(line.split(","))
