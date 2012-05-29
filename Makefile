.PHONY: all deps compile clean test xref doc check_plt build_plt clean_plt dialyze

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

doc:
	@./rebar skip_deps=true doc

APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets asn1 \
	   xmerl webtool snmp public_key mnesia eunit syntax_tools compiler otp_mibs
PLT = $(HOME)/.just_mini_plt

check_plt: compile
	dialyzer --check_plt --plt $(PLT) --apps $(APPS) deps/*/ebin

build_plt: compile
	dialyzer --build_plt --output_plt $(PLT) --apps $(APPS) deps/*/ebin

clean_plt:
	rm $(PLT)

dialyze: compile
	dialyzer -Wno_return --plt $(PLT) ebin
