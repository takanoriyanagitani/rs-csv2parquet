#!/bin/bash

icsv="./sample.d/input.csv"
opq="./sample.d/output.parquet"

geninput(){
	echo generating input file...

	mkdir -p ./sample.d

	exec 11>&1
	exec 1>"${icsv}"

	echo timestamp,severity,status,method,uri,body
	echo 2025-09-10T02:25:20.012345Z,INFO,200,GET,/index.html,helo wrld
	echo 2025-09-10T02:25:21.012345Z,INFO,200,GET,/index.html,hello

	exec 1>&11
	exec 11>&-
}

test -f "${icsv}" || geninput

echo
echo converting the csv to a parquet...
./rs-csv2parquet \
	--input-csv-filename "${icsv}" \
	--output-parquet-filename "${opq}" \
	--has-header \
	--same-column-count

echo
echo showing the parquet using rsql...
which rsql | fgrep -q rsql || exec sh -c '
	echo the rsql command not installed.
	echo you can install it using cargo.
	exit 1
'

rsql --url "parquet://${opq}" -- "SELECT * FROM output"
