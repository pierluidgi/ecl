-module(ecl_domain).
-export([
    check/1,
    check_add_cond/2,
    check_del_cond/2,
    check_manage_ring_cond/2
  ]).

-include("ecl.hrl").


%%
check({Node, Name}) ->
  case rpc:call(Node, gen_server, call, [Name, ping]) of
    pong -> ok;
    _ -> {err, {domain_not_respond, ?p(<<"Domain not respond">>)}}
  end.


%%
check_add_cond(Cluster, Domain = #{node := Node, name := Name}) -> 
  [{ecl_data, #{nodes   := Nodes, 
                domains := Domains}}] = ets:lookup(Cluster, ecl_data),
  case lists:member(Node, Nodes) of
    true ->
      case dict:is_key(ecl_misc:domain_key(Domain), Domains) of
        false -> check({Node, Name});
        true  -> {err, {already_exists, ?p(<<"domain already added. Please delete it first">>)}}
      end;
    false -> {err, {no_such_node, ?p(<<"Node not registered in cluster">>)}}
  end.


%%
check_del_cond(Cluster, Domain) -> 
  [{ecl_data, #{domains := Domains}}] = ets:lookup(Cluster, ecl_data),
  case dict:is_key(ecl_misc:domain_key(Domain), Domains) of
    true  -> 
      %% TODO check connected domains
      ok;
    false -> {err, {domain_not_found, ?p(<<"Domain not registered in cluster">>)}}
  end.


%%
check_manage_ring_cond(Cluster, Domain) ->
  [{ecl_data, #{domains := Domains}}] = ets:lookup(Cluster, ecl_data),
  case dict:is_key(ecl_misc:domain_key(Domain), Domains) of
    true  -> ok;
    false -> {err, {domain_not_registered, ?p(<<"domain not registered. Please register it first">>)}}
  end.
