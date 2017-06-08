default:
	gcc -std=gnu99 -o project -Wall -Wextra -Wno-unused-parameter -pthread cmpsc413mr.c
clean:
	rm project
