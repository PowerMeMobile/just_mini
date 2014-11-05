[![Build Status](https://travis-ci.org/PowerMeMobile/just_mini.png?branch=master)](https://travis-ci.org/PowerMeMobile/funnel_mini)
=======
### Installation & Launching

Use https://github.com/PowerMeMobile/alley-setup/ to quickly get started.

### Gateway settings

#### max_validity_period
Maximum allowed validity period (in hours).
Allowed values: positive integers, default: 48.

#### smpp_inactivity_time
Time lapse allowed between transactions, after which period
of inactivity, an SMPP entity may assume that the session
is no longer active (in milliseconds).
Allowed values: positive integers | infinity, default: infinity.

#### smpp_enquire_link_time
Time lapse allowed between operations after which an SMPP entity
should interrogate whether it’s peer still has an active
session (in milliseconds).
Allowed values: positive integers, default: 60000.

#### smpp_response_time
Time lapse allowed between an SMPP request and the corresponding
SMPP response (in milliseconds).
Allowed values: positive integers, default: 60000.

#### smpp_version
SMPP interface version to use.
Allowed values: 3.3, 3.4, 5.0, default: 3.4.

#### sar_method
Segmentation and reassembly (SAR) method to use with mulipart messages.
Allowed values: udh, sar_tlv, default: udh.

#### ip
If the host has several network interfaces, this option specifies which one to use.
If the host only has one network interface, ignore this option.
Allowed values: any ipv4 address, example: 192.168.56.1, default: undefined.

#### max_reconnect_delay
Maximum time (in milliseconds) to wait before trying to reconnect to an SMSC.
Exponential backoff will be used until this value is reached.
Allowed values: positive integers, default: 30000.

#### smpp_window_size
Maximum number of outstanding unacked SMPP requests (per SMPP connection).
Allowed values: positive integers, default: 10.

#### log_smpp_pdus
Wheter or not to log SMPP pdus.
Allowed values: true, false, default: true.

#### default_encoding
Use this encoding for {text, default}-encoded SmsRequests.
Allowed values: gsm0338, ascii, latin1, ucs2, default: gsm0338.

#### default_bitness
Use this bitness for {text, default}-encoded SmsRequests.
Allowed values: 7, 8, 16, default: 7.

#### default_data_coding
Use this data_coding for {text, default}-encoded SmsRequests.
Allowed values: non-negative integers, default: 0.

#### terminal_errors
List of SMPP errors that shouldn't be retried.
Allowed valued: comma-separated lists of base-16 integers, default: "".

#### discarded_errors
List of SMPP errors that should be ignored when retrying.
Allowed valued: comma-separated lists of base-16 integers, default: "".

#### fast_retry_times
How many times to perform a fast retry before switching to slow retries.
Allowed values: non-negative integers, default: 1.

#### slow_retry_times
How many times to do a slow retry.
Allowed values: non-negative integers, default: 1.

#### fast_retry_delay
Delay between fast retries (in milliseconds).
Allowed values: non-negative integers, default: 1000.

#### slow_retry_delay
Delay between slow retries (in milliseconds).
Allowed values: non-negative integers, default: 300000.

#### smpp_priority_flag
Default SMPP priority_flag for SUBMIT_SM.
Allowed values: 0..3, default: 0.

#### smpp_protocol_id
Default SMPP protocol_id for SUBMIT_SM.
Allowed values: non-negative integers, default: 0.
