-module(ecl_misc).

%
-compile(export_all).

-include("ecl.hrl").



%% Create 16byte size binary key from term
domain_key(#{node := Node, name := Name}) ->
  hash({Node, Name}).
hash(Data) ->
  erlang:md5(term_to_binary(Data)).

% recursion for function list
c_r(FunList, Args) ->
  case_recursion(FunList, Args).
case_recursion(FunList, Args) ->
  Fun = fun (F, [N|R], Acc = #{status := ok}) -> F(F, R, apply(N, [Acc])); (_F, _,  Acc) -> Acc end,
  Fun(Fun, FunList, Args).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%               
%% Randoms                                           
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%               
%
random_bin(N)  -> list_to_binary(random_str(N)).
%% random string
random_str(short) -> random_str(4);
random_str(long) -> random_str(8);
andom_str(Length) ->
  AllowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789",
  lists:foldl(
    fun(_, Acc) ->
      [lists:nth(rand:uniform(length(AllowedChars)), AllowedChars)] ++ Acc
    end, [], lists:seq(1, Length)).

%
random_int(1) -> 1;
random_int(N) -> rand:uniform(N).
random_int(S, T) when S > 0, T > 0, T > S -> rand:uniform(T-S+1)+S-1.


take_random_element(List) ->
  N = random_int(length(List)),
  Elem = lists:nth(N, List),
  {Elem, lists:delete(Elem, List)}.




%% Save term to dinamic module
%% for fastest access to term
%% Example:
%% >save_dynamic(Term) -> {module,dtest}.
%% >dtest:get() -> Term.
%%
save_dynamic(ModuleName, Term) ->
  save_dynamic(ModuleName, none, Term).
save_dynamic(ModuleName, Path, Term) ->
  % define the module form
  Module  = erl_syntax:attribute(erl_syntax:atom(module), [erl_syntax:atom(ModuleName)]),
  ModForm = erl_syntax:revert(Module),
  
  % define the export form
  GetFunc    = erl_syntax:arity_qualifier(erl_syntax:atom(get), erl_syntax:integer(0)),
  Export     = erl_syntax:attribute(erl_syntax:atom(export), [erl_syntax:list([GetFunc])]),
  ExportForm = erl_syntax:revert(Export),
  
  % define the function
  Clause       = erl_syntax:clause(none, [erl_syntax:abstract(Term)]),
  Function     = erl_syntax:function(erl_syntax:atom(get),[Clause]),
  FunctionForm = erl_syntax:revert(Function),
  
  {ok, Mod, Bin} = compile:forms([ModForm, ExportForm, FunctionForm]),
  {module,ModuleName} = code:load_binary(Mod, [], Bin),
  case Path /= none of
    true  -> file:write_file(Path, Bin);
    false -> do_nothing
  end,
  ok.

