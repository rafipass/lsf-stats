PIP_VERSION:=$(shell pip --version 2>/dev/null) 

all: python_lsf_api argparse

pip:
ifndef PIP_VERSION
	wget --no-check-certificate http://bootstrap.pypa.io/get-pip.py
	python get-pip.py --user
	export PATH=$PATH:$HOME/.local/bin
else
	echo "pip already installed"
endif

virtualenv: pip
	pip install --user virtualenv

lsf_env: virtualenv
	virtualenv lsf_env

python_lsf_api: lsf_env
	. lsf_env/bin/activate && pip install -e git+https://github.com/PlatformLSF/platform-python-lsf-api.git#egg=platform-python-lsf-api

argparse: lsf_env
	. lsf_env/bin/activate && pip install argparse

clean:
	rm -rf lsf_env
	rm -f get-pip.py
