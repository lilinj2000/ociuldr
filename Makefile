all: ociuldr 

ociuldr: ociuldr.c
	gcc -m32 -g -Bsymbolic -t -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -I${ORACLE_HOME}/include -L${ORACLE_HOME}/lib32 -Wl,-i -o ociuldr ociuldr.c -lm -Wl,-Bdynamic -lclntsh

clean:
	rm -rf ociuldr

