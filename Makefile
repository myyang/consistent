

test:
	go test -c && ./consistent.test -test.v

benchalloc:
	go test -c && GODEBUG=allocfreetrace=1 ./consistent.test -test.run=None -test.bench . 2>trace.log

bench:
	go test -c && ./consistent.test -test.bench .

coverage:
	go test -coverprofile cover.out && go tool cover -html cover.out
