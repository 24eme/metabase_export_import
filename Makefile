export:
	python3 metabase_export.py --verbose all

import:
	python3 metabase_import.py --verbose all

clean:
	rm -rf data
