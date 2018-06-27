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
    for p in range(a,b+1):
        # if miller_rabin(p):
        if lucas_lehmer(p):
            print('p = ', p, '  n = ', 2 ** p - 1, '\n')
    return 0
