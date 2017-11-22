with smart_open.smart_open(file,'rb') as ifile:
    for line in ifile:
            print line
            break