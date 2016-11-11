%%%----------------------------------------------------------------------
%%% File    : ecl_cluster.erl
%%% Author  : Aleksey S. Kluchnikov <alexs@ximad.com>
%%% Purpose : Clusters managment
%%% Created : 18 Mar 2016
%%%----------------------------------------------------------------------

-module(ecl_cluster).

-behaviour(gen_server).
-export([start_link/1, handle_info/2, code_change/3, terminate/2]).
-export([init/1, handle_call/3, handle_cast/2]).


-export([
    new/1, new/2,
    destroy/1,

    list/0,
    rev/1,
    show/2,

    get/2,

    add_node/2,
    del_node/2,
    sync_to_node/2,
    sync_to_all_nodes/1,
    sync_from_node/2,

    load_ets_term/2,

    load_domain/2,
    load_domain/3,
    unload_domain/3,

    copy_ring/1,
    proxy/1,
    merge/1,
    norma/1,

    checkout/1,

    add_domain/2,
    del_domain/2,

    save_to_disk/1
  ]).

-include("ecl.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen Server api
start_link([Name|_] = Args) -> gen_server:start_link({local, Name}, ?MODULE, Args, []).
handle_info(Message, State) -> io:format("Unk msg ~p~n", [{State, Message}]), {noreply, State}.
code_change(_OldVersion, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.

%%casts
handle_cast({run, _FunName, Fun, Args}, State) -> apply(Fun, [State|Args]);
handle_cast(_Req, State) -> {noreply, State}.
%%calls
handle_call({run, _FunName, Fun, Args}, From, State) -> apply(Fun, [State,From|Args]);
handle_call(_Req, _From, State) -> {reply, unknown_command, State}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


-define(DATA, #{
    rev       => undefined,
    rev_num   => 1,
    name      => undefined,
    desc      => undefined,
    status    => normal,
    nodes     => [node()],
    domains   => dict:new(),
    ring      => ecl_bisect:new(16,16),
    next_ring => ecl_bisect:new(16,16)
  }).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% init
init([Name, new]) when is_atom(Name) ->
  RevRand = ecl_misc:random_bin(8),
  Rev = <<RevRand/binary, "_1">>,
  Data = ?DATA#{
    rev  := Rev,
    name := Name
  },
  init_ets(Name, Data);
init([Name, old]) when is_atom(Name) ->
  Data = Name:get(),
  init_ets(Name, Data).

init_ets(Name, Data) ->
  ets:new(Name, [named_table, {read_concurrency, true}]),
  ets:insert(Name, {ecl_data, Data}),
  {ok, #{ets => Name}}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(ZERO_HASH, <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>).

get(ClusterName, Key) ->
  case erlang:module_loaded(ClusterName) of 
    true  -> get(ClusterName, Key, exists);
    false -> {err, {cluster_not_found, ?p(<<"Module for cluster not loaded">>)}}
  end.

get(ClusterName, Key, exists) ->
  #{status := Status, next_ring := NextRing, ring := Ring, domains := Domains} = ClusterName:get(),
  KeyHash = ecl_misc:hash(Key),
  AnswerDomains =
    case Status of
      norma -> [get_domain(Ring, Domains, KeyHash)];
      proxy -> [get_domain(R, Domains, KeyHash) || R <- [Ring, NextRing]];
      merge -> [get_domain(R, Domains, KeyHash) || R <- [Ring, NextRing]]
    end,

  F = fun
    (_Fun, [{ok, D}|[]], Acc) -> {ok, {Status, [D|Acc]}};
    ( Fun, [{ok, D}|R],  Acc) -> Fun(Fun, R, [D|Acc]);
    (_Fun, [Else|_R],   _Acc) -> Else
  end,

  case F(F, AnswerDomains, []) of
    {ok, {_, [D1, D2]}} when D1 == D2 -> {ok, {norma,[D1]}};
    Else -> Else
  end.


%
get_domain(Ring, Domains, KeyHash) ->
  case get_value_from_ring(Ring, KeyHash) of
    {ok, Value} ->
      case dict:find(Value, Domains) of
        {ok, Domain} -> {ok, Domain};
        error -> {err, {domain_not_found, ?p(<<"Domain not found, ring misconfigured">>)}}
      end;
    Else -> Else
  end.

%
get_value_from_ring(Ring, KeyHash) ->
  case ecl_bisect:next(Ring, KeyHash) of
    {_NextKey, Value} -> {ok, Value};
    not_found -> 
      case ecl_bisect:next(Ring, ?ZERO_HASH) of
        {_NextKey, Value} -> {ok, Value};
        not_found -> {err, {domain_not_found, ?p(<<"Domain not found, ring not configured">>)}}
      end
  end.
 
 


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% CLUSTERS {{{
% Start new cluster
new(ClusterName) -> 
  %% TODO start as child of top supervisor
  ChildSpec = mk_ch_sp(ClusterName, [ClusterName, new]),
  supervisor:start_child(ecl_sup, ChildSpec).
new(ClusterName, old) -> 
  %% TODO start as child of top supervisor
  ChildSpec = mk_ch_sp(ClusterName, [ClusterName, old]),
  supervisor:start_child(ecl_sup, ChildSpec).

mk_ch_sp(ClusterName, Args) ->
  #{id        => ClusterName,
    start     => {?MODULE, start_link, [Args]},
    restart   => permanent,
    shutdown  => 5000,
    type      => worker,
    modules   => [?MODULE]}.

% Destroy cluster
destroy(ClusterName) -> 
  %% TODO Save to disk and stop
  %save_to_disk(ClusterName),
  code:delete(ClusterName),
  supervisor:terminate_child(ecl_sup, ClusterName),
  supervisor:delete_child(ecl_sup, ClusterName),
  ok.


%% TODO before status change checks
% Check status normal and nodes sync
%check_proxy_cond(_ClusterName) -> ok.
% Check status 'proxy' and nodes sync
%check_merge_cond(_ClusterName) -> ok.
% Check status 'merge' and nodes sync
%check_normal_cond(_ClusterName) -> ok.

%
rev(ClusterName) ->
  case ets:info(ClusterName) of
    undefined -> {err, {cluster_not_found, ?p(<<"ets not exists">>)}};
    _ ->
      case ets:lookup(ClusterName, ecl_data) of
        [{ecl_data, #{rev := Rev}}] -> Rev;
        _ -> {err, {wrong_cluster_data, ?p(<<"Wrong data in ets">>)}}
      end
  end.
  
show(ClusterName, Type) ->
  case ets:info(ClusterName) of
    undefined -> {err, {cluster_not_found, ?p(<<"ets not exists">>)}};
    _ ->
      case ets:lookup(ClusterName, ecl_data) of
        [{ecl_data, Data}] -> show_info(Type, Data); 
        _ -> {err, {wrong_cluster_data, ?p(<<"Wrong data in ets">>)}}
      end
  end.

show_info(nodes_and_domains, #{status := Status, rev := Rev, nodes := Nodes, domains := Domains}) ->
  {ok, #{status => Status, rev => Rev, nodes => Nodes, domains => dict:to_list(Domains)}};
show_info(nodes, #{status := Status, rev := Rev, nodes := Nodes}) ->
  {ok, #{status => Status, rev => Rev, nodes => Nodes}};
show_info(domains, #{status := Status, rev := Rev, domains := Domains}) ->
  {ok, #{status => Status, rev => Rev, domains => dict:to_list(Domains)}};
show_info(rev, #{rev := Rev}) ->
  {ok, Rev};
show_info(_, _) ->
  {err, {wrong_cluster_data, ?p(<<"Wrong data in ets">>)}}.
%%}}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% NODES {{{
% Add node to cluster node list
add_node(ClusterName, Node) ->
  case ecl_node:check_add_cond(ClusterName, Node) of
    ok -> gen_server:call(ClusterName, {run, add_node_, fun add_node_/3, [Node]});
    Else -> Else
  end.
add_node_(S = #{ets := Ets}, _From, Node) ->
  [{ecl_data, Data = #{nodes := Nodes}}] = ets:lookup(Ets, ecl_data),
  NewData = Data#{nodes := lists:usort([Node|Nodes])},
  ets:insert(Ets, {ecl_data, new_rev(NewData)}),
  {reply, ok, S}.



% Del node from cluster
del_node(ClusterName, Node) ->
  case ecl_node:check_del_cond(ClusterName, Node) of
    ok -> gen_server:call(ClusterName, {run, del_node_, fun del_node_/3, [Node]});
    Else -> Else
  end.
del_node_(S = #{ets := Ets}, _From, Node) ->
  [{ecl_data, Data = #{nodes := Nodes}}] = ets:lookup(Ets, ecl_data),
  NewData = Data#{nodes := lists:delete(Node, Nodes)},
  ets:insert(Ets, {ecl_data, new_rev(NewData)}),
  {reply, ok, S}.  


sync_to_node(ClusterName, Node) ->
  case ecl_node:check_sync_cond(ClusterName, Node) of
    {ok, Term} -> 
      rpc:call(Node, ecl_cluster, load_ets_term, [ClusterName, Term]);
    Else -> Else
  end.


sync_to_all_nodes(ClusterName) -> 
  [{ecl_data, #{nodes := Nodes}}] = ets:lookup(ClusterName, ecl_data),
  [{Node, sync_to_node(ClusterName, Node)} || Node <- Nodes -- [node()]].

sync_from_node(_ClusterName, _Node) ->
  ok. %% TODO
%%}}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% DOMAINS {{{
% Add or del domain to domain dict
% Domain = {Node,DomainName},
add_domain(ClusterName, Domain) -> 
  case ecl_domain:check_add_cond(ClusterName, Domain) of
    ok   -> gen_server:call(ClusterName, {run, add_domain_, fun add_domain_/3, [Domain]});
    Else -> Else
  end.
add_domain_(S = #{ets := Ets}, _From, Domain) ->
  [{ecl_data, Data = #{domains := Domains}}] = ets:lookup(Ets, ecl_data),
  NewData = Data#{domains := dict:store(ecl_misc:domain_key(Domain), Domain, Domains)},
  ets:insert(Ets, {ecl_data, new_rev(NewData)}),
  {reply, ok, S}.

  



del_domain(ClusterName, Domain) ->
  case ecl_domain:check_del_cond(ClusterName, Domain) of
    ok   -> gen_server:call(ClusterName, {run, del_domain_, fun del_domain_/3, [Domain]});
    Else -> Else
  end.
del_domain_(S = #{ets := Ets}, _From, Domain) ->
  [{ecl_data, Data = #{domains := Domains}}] = ets:lookup(Ets, ecl_data),
  NewData = Data#{domains := dict:erase(ecl_misc:domain_key(Domain), Domains)},
  ets:insert(Ets, {ecl_data, new_rev(NewData)}),
  {reply, ok, S}.



% Load Domains to ring (Status: norma).
load_domain(ClusterName, Domain) -> load_domain(ClusterName, Domain, 16).
load_domain(ClusterName, Domain, Weigth) -> 
  case ecl_domain:check_manage_ring_cond(ClusterName, Domain) of
    ok -> gen_server:call(ClusterName, {run, load_domain_, fun load_domain_/4, [Domain, Weigth]});
    Else -> Else
  end.

load_domain_(S = #{ets := Ets}, _From, Domain, Weigth) ->
  [{ecl_data, Data = #{next_ring := Ring}}] = ets:lookup(Ets, ecl_data),
  {ok, NewRing} = ecl_ring:set_domain(Ring, Domain, Weigth),
  NewData = Data#{next_ring := NewRing},
  ets:insert(Ets, {ecl_data, new_rev(NewData)}),
  {reply, ok, S}.
  

% Unload domains from ring (Status: norma -> modified | modified -> modified)
unload_domain(ClusterName, Domain, N) ->
  case ecl_domain:check_manage_ring_cond(ClusterName, Domain) of
    ok   -> gen_server:call(ClusterName, {run, unload_domain_, fun unload_domain_/4, [Domain, N]});
    Else -> Else
  end.

unload_domain_(S = #{ets := Ets}, _From, Domain, N) ->
  [{ecl_data, Data = #{next_ring := Ring}}] = ets:lookup(Ets, ecl_data),
  {ok, NewRing} = ecl_ring:del_domain(Ring, Domain, N),
  NewData = Data#{next_ring := NewRing},
  ets:insert(Ets, {ecl_data, new_rev(NewData)}),
  {reply, ok, S}.
%%}}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% MANAGE {{{
%% Copy current ring to next_ring status shoud be norma
copy_ring(ClusterName) ->
  gen_server:call(ClusterName, {run, copy_ring_, fun copy_ring_/2, []}).
copy_ring_(S = #{ets := Ets}, _From) ->
  [{ecl_data, Data = #{ring := Ring}}] = ets:lookup(Ets, ecl_data),
  NewData = Data#{next_ring := Ring},
  ets:insert(Ets, {ecl_data, new_rev(NewData)}),
  {reply, ok, S}.



%% set status (norma -> proxy) to resolve domains to tuple Domain|{OldDomain, NewDomain}
proxy(ClusterName) -> 
  gen_server:call(ClusterName, {run, proxy_, fun proxy_/2, []}).
proxy_(S = #{ets := Ets}, _From) ->
  [{ecl_data, Data}] = ets:lookup(Ets, ecl_data),
  NewData = Data#{status := proxy},
  ets:insert(Ets, {ecl_data, new_rev(NewData)}),
  {reply, ok, S}.
  



%% set status (proxy -> merge) to resolve domains to tuple Domain|{OldDomain, NewDomain}
merge(ClusterName) ->
  gen_server:call(ClusterName, {run, merge_, fun merge_/2, []}).
merge_(S = #{ets := Ets}, _From) ->
  [{ecl_data, Data}] = ets:lookup(Ets, ecl_data),
  NewData = Data#{status := proxy},
  ets:insert(Ets, {ecl_data, new_rev(NewData)}),
  {reply, ok, S}.


%% set status (merge -> norma) to resolve domains to tuple Domain
norma(ClusterName) ->
  gen_server:call(ClusterName, {run, normal_, fun normal_/2, []}).
normal_(S = #{ets := Ets}, _From) ->
  [{ecl_data, Data = #{next_ring := Ring}}] = ets:lookup(Ets, ecl_data),
  NewData = new_rev(Data#{status := norma, ring := Ring}),
  ets:insert(Ets, {ecl_data, NewData}),
  {reply, ok, S}.


checkout(ClusterName) ->
  gen_server:call(ClusterName, {run, checkout_, fun checkout_/2, []}).
checkout_(S = #{ets := Ets}, _From) ->
  Data = Ets:get(),
  ets:insert(Ets, {ecl_data, Data}),
  {reply, ok, S}.

%%}}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% MISC {{{
save_to_disk(ClusterName) ->
  gen_server:call(ClusterName, {run, save_to_disk_, fun save_to_disk_/2, []}).
save_to_disk_(S = #{ets := Name}, _From) ->
  FileName = atom_to_list(Name) ++ ".ets",
  NodeName = atom_to_list(node()),
  ets:tab2file(Name, "../db/" ++ NodeName ++ "/" ++ FileName),
  {reply, ok, S}.


list() ->
  [N || {N,_P, _W, [ecl_cluster]} <- supervisor:which_children(ecl_sup)].


%% Update data revision
new_rev(Data = #{rev_num := Num}) ->
  RevRand = ecl_misc:random_bin(8),
  NewNum = integer_to_binary(Num+1),
  Data#{rev := <<RevRand/binary, "_", NewNum/binary>>, rev_num := Num+1}.


% Update ets data
load_ets_term(ClusterName, Term) ->
  gen_server:call(ClusterName, {run, load_ets_term_, fun load_ets_term_/3, [Term]}).
load_ets_term_(S = #{ets := Ets}, _From, Term) ->
  ets:insert(Ets, Term),
  {reply, ok, S}.
%%}}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


