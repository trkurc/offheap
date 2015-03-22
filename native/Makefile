
ARCH=win32

all:
	echo $(JAVA_HOME)
	gcc -m64 -Wall -O3 offheap.c -o offheap.dll -I "$(JAVA_HOME)/include" -I "$(JAVA_HOME)/include/$(ARCH)" -Wl,-soname,ttyutil -shared
