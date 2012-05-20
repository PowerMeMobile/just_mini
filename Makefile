.PHONY: all deps compile clean test xref

all: deps compile

deps:
	@./rebar get-deps

compile:
	@./rebar compile

clean:
	@./rebar clean

test:
	@./rebar skip_deps=true eunit

xref:
	@./rebar skip_deps=true xref
