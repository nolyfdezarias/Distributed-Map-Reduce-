build:
	echo "Instalando dependencias"
	pip install ./requirements/dill-0.3.1.1.tar.gz
	pip install ./requirements/more-itertools-8.0.0.tar.gz
	pip install ./requirements/pyzmq-18.1.1.tar.gz

Intertet:
	pip install -r requirements.txt

Server:
	python3 Master.py

Worker:
	python3 Worker.py

Client:
	python3 Client.py

