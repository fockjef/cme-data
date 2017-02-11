#!/bin/sh

if [ ! -f cme-data.db ]; then
	sqlite3 -init cme-data.sql cme-data.db .q
fi

if [ ! -f libwhaley.so ]; then
	gcc -g -fPIC -shared -O2 libwhaley.c -o libwhaley.so -lm
fi

if [ ! -d data ]; then
	mkdir data
fi



