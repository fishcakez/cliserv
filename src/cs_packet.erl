-module(cs_packet).

%% public api

-export([encode/3,
         decode/1]).

%% types

-type packet() :: {call, Id :: pos_integer(), BinReq :: binary()} |
                  {cast, BinReq :: binary()} |
                  {reply, Id :: pos_integer(), BinResp :: binary()} |
                  {exception, Id :: pos_integer(), BinReason :: binary()}.

-export_type([packet/0]).

%% public api

encode(call, Id, Binary) ->
    [<<"call", Id:64>> | Binary];
encode(cast, undefined, Binary) ->
    [<<"cast">> | Binary];
encode(reply, Id, Binary) ->
    [<<"reply", Id:64>> | Binary];
encode(exception, Id, Binary) ->
    [<<"exception", Id:64>> | Binary].

decode(<<"call", Id:64, Binary/binary>>) ->
    {call, Id, Binary};
decode(<<"cast", Binary/binary>>) ->
    {cast, Binary};
decode(<<"reply", Id:64, Binary/binary>>) ->
    {reply, Id, Binary};
decode(<<"exception", Id:64, Binary/binary>>) ->
    {exception, Id, Binary};
decode(Data) when is_binary(Data) ->
    {error, {baddata, Data}};
decode(Data) when is_list(Data) ->
    decode(iolist_to_binary(Data)).
