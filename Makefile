FLAGS :=

ifeq ($(RAW),1)
    FLAGS += --raw
endif

ifeq ($(DRY_RUN),1)
    FLAGS += --dry-run
endif

ifeq ($(VERBOSE),1)
    FLAGS += --verbose
endif

export:
	poetry run metabase_export.py ${FLAGS} all

import:
	poetry run metabase_import.py ${FLAGS} all ${COLLECTION}

clean:
	rm -rf ${MB_DATA_DIR}

format:
	poetry run isort .

lint: format
	poetry run flake8 --max-line-length=140 .
	git diff --quiet --exit-code

test:
	poetry run pytest --verbose -v -s -k $(or ${TEST_FUNC},'') .

install:
	poetry install --verbose
