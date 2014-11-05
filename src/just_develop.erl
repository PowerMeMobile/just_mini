-module(just_develop).

-export([init/0]).

-spec init() -> ok.
init() ->
    ok = application:ensure_started(sync),
    lager:set_loglevel(lager_console_backend, debug).
