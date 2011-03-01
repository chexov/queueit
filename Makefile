all:
	make q-get
	make q-put
	make q-kick
	make q-stat
	make q-wrapper
	make q-wrapper-batch

q-get:
	ln -s queueit q-get

q-put:
	ln -s queueit q-put

q-kick:
	ln -s queueit q-kick

q-stat:
	ln -s queueit q-stat

q-wrapper:
	ln -s queueit q-wrapper

q-wrapper-batch:
	ln -s queueit q-wrapper-batch

