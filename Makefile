APP=uring
CARGO_VSN=$(shell grep '^version' Cargo.toml | sed -e 's/.*=[^"]*"//' -e 's/"$$//')
VSN=$(CARGO_VSN)
YEAR=2018-2019

help:
	@echo "This makefile wraps the tasks:"
	@echo "  tarpaulin - runs code coverage tests via tarpaulin"

force:
	true

chk_copyright:
	@for f in `find . -name '*.rs' | grep -v '/target'`; do cat $$f | grep 'Copyright 2018-2020, Wayfair GmbH' > /dev/null || (echo "##[error] No copyright in $$f") done

chk_copyright_ci:
	@for f in `find . -name '*.rs' | grep -v '/target'`; do cat $$f | grep 'Copyright 2018-2020, Wayfair GmbH' > /dev/null || exit 1; done

chk_unwrap:
	@./checks/safety.sh -u

chk_unwrap_ci:
	@./checks/safety.sh -u

chk_panic:
	@./checks/safety.sh -p

chk_panic_ci:
	@./checks/safety.sh -p

docserve:
	mkdocs serve

tarpaulin:
	@docker build . -f docker/Dockerfile -t uring-tarpaulin
	@docker run --privileged --mount type=bind,source="$$(pwd)",target=/code -t uring-tarpaulin
	@echo "To view run: pycobertura show --format html --output coverage.html cobertura.xml && open coverage.html"
	@echo "  pycobertura can be installed via pip3 install pycobertura"

dep-list:
	@cargo tree --all | sed -e 's/[^a-z]*\([a-z]\)/\1/' | sort -u
