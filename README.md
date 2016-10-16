cliserv
=======

Example of building a TCP client pool (half duplex and multiplex) using sbroker.

Run
---

    $ rebar3 shell

    1> cliserver:apply(erlang, system_time, []).
