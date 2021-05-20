import datetime

val = 1611794773108299000
val = int(str(val)[:10])
x = datetime.datetime.fromtimestamp(val)
y = datetime.datetime.now()

print(x)
print(y)