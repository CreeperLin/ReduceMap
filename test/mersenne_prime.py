def lucas_lehmer(p):
    if p == 2:
        return True
    s = 4
    n = 2 ** p - 1
    for i in range(p-2):
        # s = (mod_mul(s,s,n)-2+n)%n
        s = (s*s%n-2+n)%n
    if s%n == 0:
        return True
    return False

def run(a,b):
    global output_name
    f = open(output_name,'w')
    print output_name
    for p in range(a,b+1):
        # if miller_rabin(p):
        if lucas_lehmer(p):
            print('p = '+str(p)+'  n = '+str(2 ** p - 1))
            f.write('p = '+str(p)+'  n = '+str(2 ** p - 1)+'\n')
    f.close()
    return 0
