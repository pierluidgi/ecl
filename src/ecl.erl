%%%----------------------------------------------------------------------
%%% File    : ecl.erl
%%% Author  : Aleksey S. Kluchnikov <alexs@ximad.com>
%%% Purpose : Erlang cluster mangment
%%% Created : 10 Mar 2016
%%%----------------------------------------------------------------------


-module(ecl).
-export([
    start/1, start/2,
    new/1,
    destroy/1,
    destroy_all/1,

    list/0,
    show/0, show/1, show/2,

    sync/2,

    add_node/2,
    del_node/2,
    reg_domain/2,
    unreg_domain/2,

    set_domain_to_ring/2,
    unset_domain_from_ring/2,

    get/2,

    proxy/1,
    merge/1,
    norma/1,

    commit/1, commit/2,
    checkout/1,

    force_to_normal_with_work_ring/1,
    force_to_normal_with_next_ring/1,
    flush_changes/1,
    full_check/1,

    list_domains/0, list_domains/1, list_domains/2
  ]).

-include("ecl.hrl").


%%
%% Cluster have nodes list, domains list and two rings (working ring and next_ring).
%% All changes is to next_ring, with registered damains and nodes. 
%% next_ring become work ring tru statuses: normal -> proxy -> merge -> normal
%% all changes next_ring avaliable only in normal status
%%



-type domain() :: map().
-type answer() :: ok|{ok, term()}|{err, {atom(), binary()}}.

-type cluster_name() :: atom().
-type node_name() :: atom().
-type domain_name() :: atom().
-type stat() :: [{{domain, {node_name(), domain_name()}}, ok}
                |{{node, node_name()}, ok}].


%%
%% Start claster from existing file
%%
-spec start(atom(), list()) -> answer().
start(ClusterName) ->
  Path = atom_to_list(node()) ++ "_" ++ atom_to_list(ClusterName) ++ ".beam",
  start(ClusterName, Path).
start(ModuleName, Path) ->
  case file:read_file(Path) of
    {ok, Binary} -> 
      case code:load_binary(ModuleName, [], Binary) of
        {module, ModuleName} -> 
          case ecl_cluster:new(ModuleName, old) of
            {ok, _Pid} -> ok;
            Else -> code:delete(ModuleName), {err, {start_cluster_err, ?p(Else)}}
          end;
        Else -> {err, {file_read_err, ?p(Else)}}
      end;
    {error, Reason} -> {err, {file_read_err, ?p(Reason)}}
  end.


%%
%% Start new claster with ClusterName
%% 
-spec new(atom()) -> answer().
new(ClusterName) -> 
  Funs = [
    fun f10_check_ets/1,
    fun f10_start_cluster/1
  ],
    
  F10_State = #{
    status  => ok,
    name    => ClusterName
  },

  #{status := Status} = ecl_misc:c_r(Funs, F10_State),
  Status.

%
f10_check_ets(S = #{name := ClusterName}) ->
  Status = case ets:info(ClusterName) of
    undefined -> ok;
    _ -> {err, {ets_name_buzy, ?p(<<"ClasterName ets buzy">>)}}
  end,
  S#{status := Status}.
%
f10_start_cluster(S = #{name := ClusterName}) ->
  Status = case ecl_cluster:new(ClusterName) of
    {ok, _Pid} -> ok;
    {error,{already_started, _Pid}} -> {err, {already_started, ?p(<<"ClasterName already started">>)}};
    Else -> {err, {unknown_start_err, ?p(Else)}}
  end,
  S#{status := Status}.


destroy(ClusterName) -> ecl_cluster:destroy(ClusterName).
destroy_all(ClusterName) ->
  case ets:info(ClusterName) of
    undefined -> {err, {ets_not_exists, ?p(<<"Cluster data ets not exists">>)}};
    _Else -> case ets:lookup(ClusterName, ecl_data) of
      [{ecl_data, #{nodes := Nodes}}] -> 
        [rpc:call(Node, ecl, destroy, [ClusterName]) || Node <- Nodes];
      _ -> 
        {err, {wrong_ets_data, ?p(<<"Wrong cluster data in ets">>)}}
    end
  end.

  
list() -> ecl_cluster:list().
show() ->
  List = ecl_cluster:list(),
  [#{name         => L, 
     c_rev => case show(L, compiled_rev) of {ok, V1} -> V1; E1 -> E1 end,
     m_rev  => case show(L, managed_rev)  of {ok, V2} -> V2; E2 -> E2 end} || L <- List].

%%
show(ClusterName) -> show(ClusterName, nodes_and_domains).
show(ClusterName, nodes_and_domains) -> ecl_cluster:show(ClusterName, nodes_and_domains);
show(ClusterName, nodes)             -> ecl_cluster:show(ClusterName, nodes);
show(ClusterName, domains)           -> ecl_cluster:show(ClusterName, domains);
show(ClusterName, compiled_rev)      -> #{rev := Rev} = ClusterName:get(), {ok, Rev};
show(ClusterName, managed_rev)       -> ecl_cluster:show(ClusterName, rev);
show(_ClusterName, _) -> {err, {wrong_mode, <<"nodes_and_domains|nodes|domains|compiled_rev|managed_rev only suppurted">>}}.
  

%%
%%
-spec sync(cluster_name(), all|node_name()) -> answer().
sync(ClusterName, all)  -> ecl_cluster:sync_to_all_nodes(ClusterName);
sync(ClusterName, Node) -> ecl_cluster:sync_to_node(ClusterName, Node).

%%
%%
-spec add_node(atom(), atom()) -> answer().
add_node(ClusterName, Node) -> ecl_cluster:add_node(ClusterName, Node).
-spec del_node(atom(), atom()) -> answer().
del_node(ClusterName, Node) -> ecl_cluster:del_node(ClusterName, Node).


%%
-spec reg_domain(atom(), domain())   -> answer().
reg_domain(ClusterName, Domain)    -> ecl_cluster:add_domain(ClusterName, Domain).
-spec unreg_domain(atom(), domain()) -> answer().
unreg_domain(ClusterName, Domain)  -> ecl_cluster:del_domain(ClusterName, Domain).



-spec set_domain_to_ring(atom(), domain()) -> answer().
set_domain_to_ring(ClusterName, Domain) -> set_domain_to_ring(ClusterName, Domain, 16).
-spec set_domain_to_ring(atom(), domain(), integer()) -> answer().
set_domain_to_ring(ClusterName, Domain, Weight) -> ecl_cluster:load_domain(ClusterName, Domain, Weight).
-spec unset_domain_from_ring(atom(), domain()) -> answer().
unset_domain_from_ring(ClusterName, Domain) -> set_domain_to_ring(ClusterName, Domain, 0).



%
-spec full_check(atom()) -> {answer(), stat()}. 
full_check(ClusterName) -> 
  Funs = [
    fun f100_check_ets/1,
    fun f100_check_nodes/1,
    fun f100_check_domains/1
  ],
  F100_Status = #{
    status => ok,
    name   => ClusterName,
    nodes  => [],
    domains=> [],
    stat   => []
  },
  #{status := Status, stat := Stat} = ecl_misc:c_r(Funs, F100_Status),
  {Status, Stat}.
%
f100_check_ets(S = #{name := Name}) -> 
  case ets:info(Name) of
    undefined -> S#{status := {err, {ets_not_exists, ?p(<<"Cluster data ets not exists">>)}}};
    _Else -> case ets:lookup(Name, ecl_data) of
      [{ecl_data, #{nodes := Nodes, domains := Domains}}] -> S#{nodes := Nodes, domains := Domains};
      _ -> S#{status := {err, {wrong_ets_data, ?p(<<"Wrong cluster data in ets">>)}}}
    end
  end.
%
f100_check_nodes(S = #{name := Name, nodes := [Node|Nodes], stat := Stat}) ->
  case ecl_node:check(Name, Node) of
    ok -> f100_check_nodes(S#{nodes := Nodes, stat := [{{node, Node}, ok}|Stat]});
    Else -> S#{status := Else}
  end;
f100_check_nodes(S = #{nodes := []}) -> S.

%
f100_check_domains(S = #{domains := [Domain|Domains], stat := Stat}) ->
  #{node := DNode, name := DName} = Domain,
  case ecl_domain:check({DNode, DName}) of
    ok -> f100_check_nodes(S#{domains := Domains, stat := [{{domain, {DNode, DName}}, ok}|Stat]});
    Else -> S#{status := Else} 
  end;
f100_check_domains(S = #{nodes := []}) -> S.




%% Copy work ring to next_ring
flush_changes(_ClusterName) -> ok.



%%
-spec get(atom(), term()) -> answer().
get(ClusterName, Key) -> ecl_cluster:get(ClusterName, Key).


% 
proxy(ClusterName) -> ecl_cluster:proxy(ClusterName).
merge(ClusterName) -> ecl_cluster:merge(ClusterName).
norma(ClusterName) -> ecl_cluster:norma(ClusterName).



%% Save from ets to module:get() -> Data 
commit(ClusterName) -> 
  Path = atom_to_list(node()) ++ "_" ++ atom_to_list(ClusterName) ++ ".beam",
  commit(ClusterName, Path). 
commit(ClusterName, Path) -> 
  [{ecl_data, Data}] = ets:lookup(ClusterName, ecl_data),
  ecl_misc:save_dynamic(ClusterName, Path, Data).
checkout(ClusterName) ->
  ecl_cluster:checkout(ClusterName).


%
force_to_normal_with_work_ring(_ClusterName) -> ok.
force_to_normal_with_next_ring(_ClusterName) -> ok.




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Lookups 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%
%% Domains list in ring
%%
list_domains() -> 
  Usage = <<"list_domains(ClusterName) -> list_domains(ClusterName, Src = compiled), Src::compiled|managed|compiled_new|managed_new">>,
  {usage, Usage}.

list_domains(ClusterName) ->
  list_domains(ClusterName, compiled).

list_domains(ClusterName, compiled) ->
  #{ring := Ring, domains := Domains} = ClusterName:get(),
  ecl_ring:list_domains(Ring, Domains);
list_domains(ClusterName, compiled_new) ->
  #{next_ring := Ring, domains := Domains} = ClusterName:get(),
  ecl_ring:list_domains(Ring, Domains);
list_domains(ClusterName, managed) ->
  [{ecl_data, #{ring := Ring, domains := Domains}}] = ets:lookup(ClusterName, ecl_data),
  ecl_ring:list_domains(Ring, Domains);
list_domains(ClusterName, managed_new) ->
  [{ecl_data, #{next_ring := Ring, domains := Domains}}] = ets:lookup(ClusterName, ecl_data),
  ecl_ring:list_domains(Ring, Domains).
