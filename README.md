cliserv
=======

Example of building a TCP client pool (half duplex and multiplex) using sbroker.

Run
---

    $ rebar3 shell

    1> cliserv:call(erlang, system_time, []).
    2> cliserv:cast(io, fwrite, ["hello~n"]).
