-module(ecl_node).
-export([
    check/2,
    check_add_cond/2,
    check_del_cond/2,
    check_sync_cond/2,
    check_sync/1,
    check_revs/1
  ]).

-include("ecl.hrl").


%%
check(Cluster, Node) ->
  case rpc:call(Node, application, get_application, [ecl]) of
    {ok, ecl} ->
      case lists:member(Cluster, rpc:call(Node, ecl_cluster, list, [])) of
        true  -> ok;
        false -> {err, {cluster_not_run, ?p(<<"Cluster not run on this node, run cluster first">>)}}
      end;
    undefined         -> {err, {ecl_not_run, ?p(ecl_not_run)}};
    {badrpc,nodedown} -> {err, {node_down,   ?p(node_down)}};
    Else              -> {err, {rpc_err,     ?p(Else)}}
  end.


%%
check_add_cond(Cluster, Node) -> 
  [{ecl_data, #{nodes := Nodes}}] = ets:lookup(Cluster, ecl_data),
  case lists:member(Node, Nodes) of
    false -> check(Cluster, Node);
    true  -> {err, {already_exists, ?p(<<"Node already registered, add not need">>)}}
  end.


%%
check_del_cond(Cluster, Node) -> 
  [{ecl_data, #{nodes := Nodes, domains := Domains}}] = ets:lookup(Cluster, ecl_data),
  case lists:member(Node, Nodes) of
    true  ->
      case [D || {_K,{N, D}} <- Domains, N == Node] == [] of
        true  -> ok;
        false -> {err, {wired_domains, ?p(<<"Node have registered domains, delete it first">>)}}
      end;
    false -> {err, {node_not_found, ?p(node_not_found)}}
  end.


%%
check_sync_cond(Cluster, Node) ->
  [Term = {ecl_data, #{nodes := Nodes}}] = ets:lookup(Cluster, ecl_data),
  case lists:member(Node, Nodes) of
    true ->
      case lists:member(Cluster, rpc:call(Node, ecl_cluster, list, [])) of
        true  -> {ok, Term};
        false -> {err, {cluster_not_run, ?p(<<"Cluster not run on this node, run cluster first">>)}}
      end;
    false -> {err, {node_not_exist, ?p(<<"Node not registered">>)}}
  end.


check_sync(ClusterName) ->
  [{ecl_data, #{nodes := AllNodes, rev := Rev}}] = ets:lookup(ClusterName, ecl_data),
  Nodes = AllNodes -- [node()],
  F = fun
        (N, true)  -> rpc:call(N, ecl_cluster, rev, [ClusterName]) == Rev;
        (_, false) -> false
      end,
  lists:foldl(F, true, Nodes).


check_revs(ClusterName) ->
  case ets:info(ClusterName) of
    undefined -> {err, {no_such_cluster, ?p(<<"Cluster not exists">>)}};
    _ ->
      case ets:lookup(ClusterName, ecl_data) of
        [{ecl_data, #{nodes := Nodes}}] ->
          F = fun(N) -> {N, rpc:call(N, ecl_cluster, rev, [ClusterName])} end,
          {ok, [F(N) || N <- Nodes]};
        _ -> {err, {wrong_data, ?p(<<"Wrong cluster ets data">>)}}
      end
  end.

