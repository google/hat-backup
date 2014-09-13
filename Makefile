LIBS=-L quickcheck -L rustsqlite -L sodiumoxide
RUST=rustc ${LIBS}

SRC=src/hat

all: bin/hat

clean:
	rm -f bin/hat*



dep-quickcheck: quickcheck/src/*.rs
	${RUST} --crate-type lib -O quickcheck/src/lib.rs && mv libquickcheck*.rlib quickcheck/

dep-sodiumoxide: sodiumoxide/src/sodiumoxide/*.rs
	${RUST} --crate-type lib -O sodiumoxide/src/sodiumoxide/lib.rs && mv libsodiumoxide*.rlib sodiumoxide/

dep-rustsqlite: rustsqlite/src/*.rs
	${RUST} --crate-type lib -O rustsqlite/src/sqlite3.rs && mv libsqlite*.rlib rustsqlite/

deps: dep-quickcheck dep-sodiumoxide dep-rustsqlite



analysis: ${SRC}/*.rs
	${RUST} --no-trans ${SRC}/main.rs

bin/hat: ${SRC}/*.rs
	${RUST} -o bin/hat ${SRC}/main.rs

lib: ${SRC}/*.rs
	${RUST} ${SRC}/lib.rs

bin/hat-opt: ${SRC}/*.rs
	${RUST} -O -o bin/hat-opt ${SRC}/main.rs

bin/hat-opt1: ${SRC}/*.rs
	${RUST} --opt-level=1 -o bin/hat-opt1 ${SRC}/main.rs

bin/hat-opt3: ${SRC}/*.rs
	${RUST} --opt-level=3 -o bin/hat-opt3 ${SRC}/main.rs

bin/hat-debug: ${SRC}/*.rs
	${RUST} -g -O -o bin/hat-debug ${SRC}/main.rs

bin/hat-test: ${SRC}/*.rs
	${RUST} -O -o bin/hat-test --test ${SRC}/lib.rs

test: bin/hat-test
	./bin/hat-test

bench: bin/hat-test
	./bin/hat-test --bench

doc: ${SRC}/*.rs
	rustdoc ${LIBS} ${SRC}/lib.rs
