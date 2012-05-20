-module(just_settings).
-export([cast/2]).
-export([get/2, default_value/1]).

-type name() :: max_validity_period | smpp_inactivity_time |
                smpp_enquire_link_time | smpp_response_time | smpp_version |
                sar_method | ip | max_reconnect_delay | smpp_window_size |
                log_smpp_pdus |
                % SETTINGS THAT MIGHT NOT SURVIVE REFACTORING
                default_encoding | default_bitness | default_data_coding |
                terminal_errors | discarded_errors | fast_retry_times |
                slow_retry_times | fast_retry_delay | slow_retry_delay |
                smpp_priority_flag | smpp_protocol_id.

-type setting() :: {name(), any()}.
-type settings() :: [setting()].

-export_type([settings/0]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec cast(string(), string()) -> setting() | 'error'.
cast(Key, Value) ->
    case cast({Key, Value}) of
        error ->
            error;
        Cast ->
            {list_to_atom(Key), Cast}
    end.

-spec get(name(), settings()) -> any().
get(Name, Settings) ->
    proplists:get_value(Name, Settings, default_value(Name)).

-spec default_value(name()) -> any().
default_value(max_validity_period)    -> 48;       % 2 days.
default_value(smpp_inactivity_time)   -> infinity; % no timeout, never drop the session.
default_value(smpp_enquire_link_time) -> 60000;    % 1 minute.
default_value(smpp_response_time)     -> 60000;    % 1 minute.
default_value(smpp_version)           -> 16#34;    % 3.4.
default_value(sar_method)             -> udh;
default_value(ip)                     -> undefined;
default_value(max_reconnect_delay)    -> 30000; % 30 seconds.
default_value(smpp_window_size)       -> 10;
%% SETTINGS THAT MIGHT NOT SURVIVE REFACTORING
default_value(default_encoding)       -> gsm0338;
default_value(default_bitness)        -> 7;
default_value(default_data_coding)    -> 0;
default_value(log_smpp_pdus)          -> true;
default_value(terminal_errors)        -> [];
default_value(discarded_errors)       -> [];
default_value(fast_retry_times)       -> 1;
default_value(slow_retry_times)       -> 1;
default_value(fast_retry_delay)       -> 1000;   % 1 second.
default_value(slow_retry_delay)       -> 300000; % 5 minutes.
default_value(smpp_priority_flag)     -> 0;
default_value(smpp_protocol_id)       -> 0.

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

cast({"max_validity_period", Value}) ->
    cast_pos_integer(Value);
cast({Name, Value}) when Name =:= "smpp_enquire_link_time";
                         Name =:= "smpp_response_time" ->
    cast_pos_integer(Value);
cast({"smpp_inactivity_time", Value}) ->
    case Value of
        "infinity" -> infinity;
        _          -> cast_pos_integer(Value)
    end;
cast({"smpp_version", Value}) ->
    case Value of
        "3.3" -> 16#33;
        "3.4" -> 16#34;
        "5.0" -> 16#50;
        _     -> error
    end;
cast({"sar_method", Value}) when Value =:= "udh";
                                 Value =:= "sar_tlv" ->
    list_to_atom(Value);
cast({"ip", Value}) ->
    case inet_parse:address(Value) of
        {ok, IpAddr} ->
            IpAddr;
        {error, einval} ->
            error
    end;
cast({"max_reconnect_delay", Value}) ->
    cast_pos_integer(Value);
cast({"smpp_window_size", Value}) ->
    cast_pos_integer(Value);
cast({"log_smpp_pdus", Value}) when Value =:= "true";
                                    Value =:= "false" ->
    list_to_atom(Value);
%% SETTINGS THAT MIGHT NOT SURVIVE REFACTORING
cast({"default_encoding", Value}) when Value =:= "gsm0338";
                                       Value =:= "ascii";
                                       Value =:= "latin1";
                                       Value =:= "ucs2" ->
    list_to_atom(Value);
cast({"default_data_coding", Value}) ->
    cast_non_neg_integer(Value);
cast({"default_bitness", Value}) when Value =:= "7";
                                      Value =:= "8";
                                      Value =:= "16" ->
    list_to_integer(Value);
cast({Name, Value}) when Name =:= "terminal_errors";
                         Name =:= "discarded_errors" ->
    Split = string:tokens(Value, ","),
    case lists:all(fun(P) -> nomatch =/= re:run(P, "^[0-9A-Fa-f]+$") end, Split) of
        true ->
            [ erlang:list_to_integer(E, 16) || E <- Split ];
        false ->
            error
    end;
cast({Name, Value}) when Name =:= "fast_retry_times";
                         Name =:= "slow_retry_times";
                         Name =:= "fast_retry_delay";
                         Name =:= "slow_retry_delay" ->
    cast_non_neg_integer(Value);
cast({"smpp_priority_flag", Value}) ->
    case cast_non_neg_integer(Value) of
        N when is_integer(N), N >= 0, N =< 3 ->
            N;
        _ ->
            error
    end;
cast({"smpp_protocol_id", Value}) ->
    cast_non_neg_integer(Value);
cast({_Name, _Value}) ->
    error.

cast_pos_integer(Value) ->
    case cast_integer(Value) of
        N when is_integer(N), N > 0 ->
            N;
        _ ->
            error
    end.

cast_non_neg_integer(Value) ->
    case cast_integer(Value) of
        N when is_integer(N), N >= 0 ->
            N;
        _ ->
            error
    end.

cast_integer(Value) ->
    try
        list_to_integer(Value)
    catch
        error:badarg ->
            error
    end.
