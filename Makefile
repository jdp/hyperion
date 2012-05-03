init:
	pip install -r requirements.txt

test:
	nosetests tests

testdebug:
	nosetests -s tests
