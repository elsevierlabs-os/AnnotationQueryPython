help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  install            to install required modules"
	@echo "  uninstall          to uninstall required modules"
	@echo "  sdist              to create a source distribution"
	@echo "  wheel              to create a wheel distribution"
	@echo "  egg                to create an egg distribution"
	@echo "  test               to run all of the test suites"
	@echo "  testQuery          to run the query test suite"
	@echo "  testConcordancers  to run the concordancer test suite"
	@echo "  testUtilities      to run the utilities test suite"			

install:
	pip install -r requirements.txt

uninstall:
	pip uninstall -y -r requirements.txt

sdist:
	python setup.py sdist

wheel:
	pip install wheel
	python setup.py bdist_wheel 	

egg:
	python setup.py bdist_egg

test:
	nosetests  --nocapture tests/test_*.py

testQuery:
	nosetests  --nocapture tests/test_query.py

testConcordancers:
	nosetests  --nocapture tests/test_concordancers.py

testUtilities:
	nosetests  --nocapture tests/test_utilities.py