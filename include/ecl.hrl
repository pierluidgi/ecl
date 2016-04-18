

%% Print message with module and line number point
-define(p(Str), list_to_binary(io_lib:format("Mod:~w line:~w ~100P~n", [?MODULE,?LINE, Str, 300]))).

