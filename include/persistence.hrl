%%% Records serialized in tokyo cabinet hash tables.

%% -record(addr,
%%         {addr :: string(),
%%          ton :: non_neg_integer(),
%%          npi :: non_neg_integer()}).

-include_lib("alley_dto/include/addr.hrl").

-record(message,
        {orig :: #addr{},
         dest :: #addr{},
         body :: binary(),
         data_coding :: non_neg_integer(),
         sar_total_segments = 1 :: non_neg_integer(),
         sar_segment_seqnum :: pos_integer() | undefined,
         sar_msg_ref_num :: pos_integer() | undefined,
         accepted_at :: just_time:precise_time()}).

-record(receipt,
        {orig :: #addr{},
         message_id :: binary(),
         message_state :: atom(),
         accepted_at :: just_time:precise_time()}).

-record(segment_result,
        {sar_segment_seqnum :: pos_integer() | undefined,
         orig_msg_id :: binary(),
         result :: {ok, binary()} | {error, integer()}}).

-record(response,
        {batch :: binary(), % original batch UUID.
         customer :: binary(), % customer UUID.
         dest :: #addr{},
         sar_total_segments = 1 :: pos_integer(),
         segment_results :: [#segment_result{}],
         accepted_at :: just_time:precise_time()}).

-record(short_info,
        {orig_msg_id :: binary()}).

-record(long_info,
        {orig_msg_ids :: [binary()],
         sar_msg_ref_num :: non_neg_integer()}).

-record(segment_info,
        {orig_msg_id :: binary(),
         sar_msg_ref_num :: non_neg_integer(),
         sar_total_segments :: pos_integer(),
         sar_segment_seqnum :: pos_integer()}).

-record(port_addressing,
        {dest :: non_neg_integer(),
         orig :: non_neg_integer()}).

-record(request,
        {batch :: binary(), % batch UUID.
         customer :: binary(), % customer UUID.
         type :: short | long | segment,
         attempt_once :: boolean(),
         info :: #short_info{} | #long_info{} | #segment_info{},
         payload :: [binary()],
         data_coding :: non_neg_integer(),
         orig :: #addr{},
         dest :: #addr{},
         done_segments = [] :: [#segment_result{}],
         todo_segments :: [pos_integer()],
         validity_period :: string(),
         service_type :: string() | undefined,
         protocol_id :: non_neg_integer() | undefined,
         priority_flag :: non_neg_integer() | undefined,
         registered_delivery :: 0..2,
         port_addressing :: #port_addressing{} | undefined,
         attempts = 0 :: non_neg_integer(),
         attempt_at :: just_time:precise_time(),
         expires_at :: just_time:precise_time(),
         accepted_at :: just_time:precise_time()}).
