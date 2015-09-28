-record(smpp_connection, {
    id          :: integer(),
    type        :: transmitter | receiver | transceiver,
    addr        :: string(),
    port        :: 0..65535,
    system_id   :: string(),
    password    :: string(),
    system_type :: string(),
    addr_ton    :: 0..6,
    addr_npi    :: 0..14,
    addr_range  :: string()
}).

-record(gateway, {
    uuid        :: binary(),
    name        :: string(),
    rps         :: pos_integer(),
    settings    :: just_settings:settings(),
    connections :: [#smpp_connection{}]}).

-record(connection_state, {
    id          :: integer(),
    type        :: transmitter | receiver | transceiver,
    addr        :: string(),
    port        :: 0..65535,
    system_id   :: string(),
    password    :: string(),
    system_type :: string(),
    state       :: connected | connecting
}).

-record(gateway_state, {
    uuid        :: binary(),
    name        :: string(),
    host        :: string(),
    state       :: started | stopped,
    connections :: [#connection_state{}]
}).
