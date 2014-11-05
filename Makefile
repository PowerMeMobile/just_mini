.PHONY: all deps compile clean test xref doc check_plt build_plt clean_plt dialyze

NAME=just_mini
OTP_PLT=~/.otp.plt
PRJ_PLT=$(NAME).plt

all: generate

generate: compile xref
	@rm -rf ./rel/$(NAME)
	@./rebar generate

compile: get-deps
	@./rebar compile

get-deps:
	@./rebar get-deps

update-deps:
	@./rebar update-deps

xref: compile
	@./rebar xref skip_deps=true

clean:
	@./rebar clean

dialyze: $(OTP_PLT) compile $(PRJ_PLT)
	@dialyzer --plt $(PRJ_PLT) -r ./subapps/*/ebin

$(OTP_PLT):
	@dialyzer --build_plt --output_plt $(OTP_PLT) --apps erts \
		kernel stdlib crypto mnesia sasl common_test eunit ssl \
		asn1 compiler syntax_tools inets

$(PRJ_PLT):
	@dialyzer --add_to_plt --plt $(OTP_PLT) --output_plt $(PRJ_PLT) \
	-r ./deps/*/ebin ./subapps/*/ebin

console:
	@./rel/$(NAME)/bin/just console

develop:
	@./rel/$(NAME)/bin/just develop

test:
	@./rebar skip_deps=true eunit

doc:
	@./rebar skip_deps=true doc

tags:
	@find . -name "*.[e,h]rl" -print | etags -
