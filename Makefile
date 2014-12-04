all: lsf_install web_install
	
web_install: 
	cd web && npm install .

lsf_install:
	cd lsf_bindings && make all
