-module(just_mib).

-include("gateway.hrl").
-include("customer.hrl").
-include("JUST-MIB.hrl").

-export([ensure_mnesia_tables/1]).
-export([gateways/0, customers/0, customer/1]).
-export([send_coldstart_notification/0]).
-export([gateways_table/4, settings_table/4,
         connections_table/4,
         customers_table/4]).

-record(gatewaysTable, {uuid, name, rps, status}).
-record(settingsTable, {index, value, status}).
-record(connectionsTable, {index, type, addr, port, system_id, password,
                           system_type, addr_ton, addr_npi, addr_range, status}).
-record(customersTable, {uuid, rps, priority, status}).

-define(gv(Key, Params), proplists:get_value(Key, Params)).

%% -------------------------------------------------------------------------
%% mnesia tables creation
%% -------------------------------------------------------------------------

%% TODO: if a table exists, ensure it has same attributes.
ensure_mnesia_tables(Timeout) ->
    mnesia:create_table(gatewaysTable,
                        [{snmp, [{key, fix_string}]},
                         {disc_copies, [node()]},
                         {attributes, record_info(fields, gatewaysTable)}]),
    mnesia:create_table(settingsTable,
                        [{snmp, [{key, {fix_string, string}}]},
                         {disc_copies, [node()]},
                         {attributes, record_info(fields, settingsTable)}]),
    mnesia:create_table(connectionsTable,
                        [{snmp, [{key, {fix_string, integer}}]},
                         {disc_copies, [node()]},
                         {attributes, record_info(fields, connectionsTable)}]),
    mnesia:create_table(customersTable,
                        [{snmp, [{key, fix_string}]},
                         {disc_copies, [node()]},
                         {attributes, record_info(fields, customersTable)}]),
    mnesia:wait_for_tables([gatewaysTable, settingsTable, customersTable],
                           Timeout).

%% -------------------------------------------------------------------------
%%
%% -------------------------------------------------------------------------

-spec gateways() -> [#'gateway'{}].
gateways() ->
    GtwMatch = #gatewaysTable{uuid = '$1', name = '$2', rps = '$3',
                              status = ?gtwStatus_active},
    StsMatch = #settingsTable{index = {'$1', '$2'}, value = '$3',
                              status = ?stsStatus_active},
    CnnMatch = #connectionsTable{index = {'$1', '$2'}, type = '$3', addr = '$4',
                                 port = '$5', system_id = '$6', password = '$7',
                                 system_type = '$8', addr_ton = '$9',
                                 addr_npi = '$10', addr_range = '$11',
                                 status = ?cnnStatus_active},
    F = fun() ->
            {mnesia:select(gatewaysTable, [{GtwMatch, [], [['$1', '$2', '$3']]}]),
             mnesia:select(settingsTable, [{StsMatch, [], [['$1', '$2', '$3']]}]),
             mnesia:select(connectionsTable, [{CnnMatch, [],
                                               [['$1', '$2', '$3', '$4', '$5',
                                                 '$6', '$7', '$8', '$9', '$10',
                                                 '$11']]}])}
        end,
    {AllGtws, AllSts, AllCnns} = mnesia:activity(transaction, F),
    lists:map(fun([UUID, Name, RPS]) ->
                  Sts = [ just_settings:cast(StsName, StsValue)
                          || [GtwUUID, StsName, StsValue]
                          <- AllSts, GtwUUID =:= UUID ],
                  Cns = [ begin
                              {value, Type} = snmpa:int_to_enum(cnnType, CnnType),
                              #smpp_connection{id = CnnId,
                                               type = Type,
                                               addr = CnnAddr,
                                               port = CnnPort,
                                               system_id = CnnSystemId,
                                               password = CnnPassword,
                                               system_type = CnnSystemType,
                                               addr_ton = CnnAddrTon,
                                               addr_npi = CnnAddrNpi,
                                               addr_range = CnnAddrRange}
                          end
                          || [GtwUUID, CnnId, CnnType, CnnAddr, CnnPort,
                              CnnSystemId, CnnPassword, CnnSystemType,
                              CnnAddrTon, CnnAddrNpi, CnnAddrRange]
                          <- AllCnns, GtwUUID =:= UUID ],
                  #gateway{uuid = uuid:parse(list_to_binary(UUID)), name = Name, rps = RPS,
                           settings = Sts, connections = Cns}
              end, AllGtws).

-spec customers() -> [#'customer'{}].
customers() ->
    Match= #customersTable{uuid = '$1', rps = '$2', priority = '$3',
                           status = ?cstStatus_active},
    F = fun() ->
            mnesia:select(customersTable, [{Match, [], [['$1', '$2', '$3']]}])
        end,
    lists:map(fun([UUID, RPS, Priority]) ->
                  #customer{uuid = uuid:parse(list_to_binary(UUID)), rps = RPS,
                            priority = Priority}
              end, mnesia:activity(transaction, F)).

-spec customer(binary()) -> #customer{} | undefined.
customer(UUID) ->
    Unparsed = binary_to_list(uuid:unparse(UUID)),
    case mnesia:activity(transaction,
                         fun() -> mnesia:read(customersTable, Unparsed) end) of
        [] ->
            undefined;
        [#customersTable{rps = RPS, priority = Priority}] ->
            #customer{uuid = UUID, rps = RPS, priority = Priority}
    end.

%% -------------------------------------------------------------------------
%% notifications
%% -------------------------------------------------------------------------

-spec send_coldstart_notification() -> no_return().
send_coldstart_notification() ->
    snmpa:send_notification(snmp_master_agent, coldStart, no_receiver,
                            "coldStart", []).

%% -------------------------------------------------------------------------
%% gatewaysTable
%% -------------------------------------------------------------------------

gateways_table(set, RowIndex, Cols, NameDb) ->
    % TODO: remove all dependent settings upon destroy.
    % TODO: remove all dependent connections upon destroy.
    Res = snmp_generic:table_func(set, RowIndex, Cols, NameDb),
    just_gateways:update(),
    Res;
%% is_set_ok, undo, get, and get_next.
gateways_table(Op, RowIndex, Cols, NameDb) ->
    snmp_generic:table_func(Op, RowIndex, Cols, NameDb).

%% -------------------------------------------------------------------------
%% settingsTable
%% -------------------------------------------------------------------------

settings_table(is_set_ok, RowIndex, Cols, NameDb) ->
    % TODO: return noCreation error for bad names
    Name = setting_name(RowIndex),
    Value = ?gv(?stsValue, Cols),
    case Value =:= undefined orelse just_settings:cast(Name, Value) =/= error of
        true ->
            snmp_generic:table_func(is_set_ok, RowIndex, Cols, NameDb);
        false ->
            {wrongValue, ?stsValue}
    end;
settings_table(set, RowIndex, Cols, NameDb) ->
    % TODO: validate gateway index part.
    Res = snmp_generic:table_func(set, RowIndex, Cols, NameDb),
    just_gateways:update(),
    Res;
%% undo, get and get_next.
settings_table(Op, RowIndex, Cols, NameDb) ->
    snmp_generic:table_func(Op, RowIndex, Cols, NameDb).

setting_name(RowIndex) ->
    element(2, setting_mnesia_index(RowIndex)).

setting_mnesia_index(RowIndex) ->
    {GtwUUID, [_Len|StsName]} = lists:split(36, RowIndex),
    {GtwUUID, StsName}.

%% -------------------------------------------------------------------------
%% connectionsTable
%% -------------------------------------------------------------------------

connections_table(set, RowIndex, Cols, NameDb) ->
    % TODO: validate gateway uuid.
    Res = snmp_generic:table_func(set, RowIndex, Cols, NameDb),
    just_gateways:update(),
    Res;
%% is_set_ok, undo, get and get_next.
connections_table(Op, RowIndex, Cols, NameDb) ->
    snmp_generic:table_func(Op, RowIndex, Cols, NameDb).

%% -------------------------------------------------------------------------
%% customersTable
%% -------------------------------------------------------------------------

customers_table(set, RowIndex, Cols, NameDb) ->
    Res = snmp_generic:table_func(set, RowIndex, Cols, NameDb),
    just_customers:update(uuid:parse(list_to_binary(RowIndex))),
    Res;
%% is_set_ok, undo, get and get_next.
customers_table(Op, RowIndex, Cols, NameDb) ->
    snmp_generic:table_func(Op, RowIndex, Cols, NameDb).
