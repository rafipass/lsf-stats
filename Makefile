all: lsf_install web_install
	
web_install: 
	cd web && npm install .
	cd web && git update-index --assume-unchanged config.json
lsf_install:
	cd lsf_bindings && make all
