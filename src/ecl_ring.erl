%%%----------------------------------------------------------------------
%%% File    : ecl_ring.erl
%%% Author  : Aleksey S. Kluchnikov <alexs@ximad.com>
%%% Purpose : Ring managment
%%% Created : 28 Mar 2016
%%%----------------------------------------------------------------------


-module(ecl_ring).

-export([
    set_domain/2, set_domain/3,
    del_domain/2
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


del_domain(Ring, Domain) ->
  set_domain(Ring, Domain, 0).
%
del_keys(Ring, N, Keys) when N > 0->
  {Key, RestKeys} = ecl_misc:take_random_element(Keys),
  NewRing = ecl_bisect:delete(Ring, Key),
  del_keys(NewRing, N-1, RestKeys);
del_keys(Ring, N, _Keys) when N == 0 -> 
  {ok, Ring}.


  

