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
	python3 metabase_export.py ${FLAGS} all

import:
	python3 metabase_import.py ${FLAGS} all ${COLLECTION}

clean:
	rm -rf ${MB_DATA_DIR}

test:
	python3 -m pytest --verbose -v -s -k $(or ${TEST_FUNC},'') .
