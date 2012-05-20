-module(just_helpers).

-export([split_binary/2]).

%% -------------------------------------------------------------------------
%% binaries
%% -------------------------------------------------------------------------

-spec split_binary(binary(), pos_integer()) -> [binary()].
split_binary(Bin, Len) when Len > 0 ->
    split_binary(Bin, Len, []).

split_binary(Bin, Len, Acc) ->
    case size(Bin) of
        0 ->
            lists:reverse(Acc);
        N when N =< Len ->
            lists:reverse([Bin | Acc]);
        _ ->
            {Bin1, Bin2} = erlang:split_binary(Bin, Len),
            split_binary(Bin2, Len, [Bin1 | Acc])
    end.
