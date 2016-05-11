%%%----------------------------------------------------------------------
%%% File    : ecl_ring.erl
%%% Author  : Aleksey S. Kluchnikov <alexs@ximad.com>
%%% Purpose : Ring managment
%%% Created : 28 Mar 2016
%%%----------------------------------------------------------------------


-module(ecl_ring).

-export([
    set_domain/2, set_domain/3,
    del_domain/3,
    list_domains/2,
    list_nodes/2,
    different_domains_on_nodes/2
  ]).



set_domain(Ring, Domain) ->
  set_domain(Ring, Domain, 16). 

set_domain(Ring, Domain, Weigth) -> 
  DomainKey = ecl_misc:domain_key(Domain),
  add_keys(Ring, Weigth, DomainKey).

add_keys(Ring, N, DomainKey) when N > 0 ->
  Key = ecl_misc:hash(ecl_misc:random_bin(16)),
  NewRing = ecl_bisect:insert(Ring, Key, DomainKey),
  add_keys(NewRing, N - 1, DomainKey);
add_keys(Ring, N, _DomainKey) when N == 0 ->
  {ok, Ring}.


del_domain(Ring, Domain, N) ->
  DomainKey = ecl_misc:domain_key(Domain),
  F = fun
    (K, V, Acc) when V == DomainKey  -> [K|Acc];
    (_, _, Acc) -> Acc
  end,
  Keys = ecl_bisect:foldl(Ring, F, []),
  del_keys(Ring, N, Keys).


%
del_keys(Ring, N, Keys) when N > 0->
  {Key, RestKeys} = ecl_misc:take_random_element(Keys),
  NewRing = ecl_bisect:delete(Ring, Key),
  del_keys(NewRing, N-1, RestKeys);
del_keys(Ring, N, _Keys) when N == 0 -> 
  {ok, Ring}.


%%
list_domains(Ring, Domains) ->
  F = 
    fun(_Key, Value, Acc) -> 
      Counter = proplists:get_value(Value, Acc, 0),
      lists:keystore(Value, 1, Acc, {Value, Counter+1})
    end,
  List = ecl_bisect:foldl(Ring, F, []),
  [{dict:find(DKey, Domains), Len} || {DKey, Len} <- List].
 

%%
list_nodes(Ring, Domains) ->
  List = list_domains(Ring, Domains),
  F = fun
    ({{ok, #{node := Node}}, Len}, Acc) ->
        Counter = proplists:get_value(Node, Acc, 0),
        lists:keystore(Node, 1, Acc, {Node, Counter + Len});
    (_, Acc) -> Acc
  end,
  lists:foldl(F, [], List).


%%
different_domains_on_nodes(Ring, Domains) ->
  List = list_domains(Ring, Domains),
  F = fun
    ({{ok, #{node := Node}}, _Len}, Acc) ->
        Counter = proplists:get_value(Node, Acc, 0),
        lists:keystore(Node, 1, Acc, {Node, Counter+1});
    (_, Acc) -> Acc
  end,
  lists:foldl(F, [], List).
