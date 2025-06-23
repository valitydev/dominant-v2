-module(cthr).

%% Stub module for dialyzer to prevent unknown function warnings
%% The real cthr module comes from cth_readable parse transform

-export([pal/1, pal/2, pal/3, pal/4, pal/5]).

-spec pal(term()) -> ok.
pal(_) -> ok.

-spec pal(term(), term()) -> ok.
pal(_, _) -> ok.

-spec pal(term(), term(), term()) -> ok.
pal(_, _, _) -> ok.

-spec pal(term(), term(), term(), term()) -> ok.
pal(_, _, _, _) -> ok.

-spec pal(term(), term(), term(), term(), term()) -> ok.
pal(_, _, _, _, _) -> ok.
