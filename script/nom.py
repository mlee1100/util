import sys
import time
import os
os.system('setterm -cursor off')
try:
	i = 0
	nomarray = []
	while i <= 100:
		i += 1
		nomarray.append('nom')
		nomstring = ' '.join(nomarray)
		print(nomstring, end='\r')
		if len(nomarray) >= 3:
			nomarray = []
			time.sleep(.2)
			print('                   ', end='\r')
		time.sleep(.15)
		# print '\r',
except KeyboardInterrupt:
	sys.exit(0) 
finally:
	os.system('setterm -cursor on')