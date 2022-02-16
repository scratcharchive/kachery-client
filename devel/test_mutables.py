import kachery_client as kc

key = {'name': 'test-key-1'}
kc.set(key, True)
a = kc.get(key)
assert a == True

r = kc.set(key, False, update=False)
assert r == False
a = kc.get(key)
assert a == True

r = kc.set(key, False, update=True)
assert r == True
a = kc.get(key)
assert a == False

x = kc.delete(key)
assert x == True
r = kc.set(key, True, update=False)
assert r == True
a = kc.get(key)
assert a == True

x = kc.delete(key)
assert x == True
y = kc.delete(key)
assert y == False