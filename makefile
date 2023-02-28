MAIN = main # main or master

help: # default target
	@echo "Hello"

lint: # execute sqlfluff lint on all modified models (different from main branch) !!!! `git add .` required !!!!
	git diff $(MAIN) --name-only --diff-filter=d | egrep '^models/.*sql$$' | xargs -r sqlfluff lint
	
fix: # execute sqlfluff fix on all modified models (different from main branch) !!!! `git add .` required !!!!
	git diff $(MAIN) --name-only --diff-filter=d | egrep '^models/.*sql$$' | xargs -r sqlfluff fix

git_diff_model: # view all modified models (different from main branch) !!!! `git add .` required !!!! 
	git diff $(MAIN) --name-only --diff-filter=d | egrep '^models/.*sql$$'

build: # equivalent to `dbt build --select state:modified+` !!!! `git add .` required !!!!
	git diff $(MAIN) --name-only --diff-filter=d | egrep '^models/.*sql$$' | awk -F '.' "{print $$1}" | awk -F '/' "{print $$NF}" | sed ':a;N;$$!ba;s/\n/+ /g' | xargs -I % dbt build -s %+

required: # check for model that don't have enough test implemented
	dbt run-operation required_tests

dag: # generate and spin up dbt docs locally
	dbt docs generate && dbt docs serve

serve: # spin up dbt docs locally
	dbt docs serve
