-module(dmt_author_handler).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

%% API
-export([handle_function/4]).

handle_function(Function, Args, WoodyContext0, Options) ->
    DefaultDeadline = woody_deadline:from_timeout(default_handling_timeout(Options)),
    WoodyContext = dmt_api_woody_utils:ensure_woody_deadline_set(WoodyContext0, DefaultDeadline),
    do_handle_function(Function, Args, WoodyContext, Options).

default_handling_timeout(#{default_handling_timeout := Timeout}) ->
    Timeout.

%% Implement the Create function
do_handle_function('Create', {Params}, _Context, _Options) ->
    #domain_conf_v2_AuthorParams{email = Email, name = Name} = Params,
    case dmt_author:insert(Name, Email) of
        {ok, {already_exists, ExistingID}} ->
            woody_error:raise(business, #domain_conf_v2_AuthorAlreadyExists{id = ExistingID});
        {ok, ID} ->
            {ok, #domain_conf_v2_Author{id = ID, email = Email, name = Name}};
        {error, Reason} ->
            woody_error:raise(system, {internal, Reason})
    end;
do_handle_function('Get', {AuthorID}, _Context, _Options) ->
    case dmt_author:get(AuthorID) of
        {ok, Author} ->
            {ok, Author};
        {error, author_not_found} ->
            woody_error:raise(business, #domain_conf_v2_AuthorNotFound{});
        {error, Reason} ->
            woody_error:raise(system, {internal, Reason})
    end;
do_handle_function('Delete', {AuthorID}, _Context, _Options) ->
    case dmt_author:delete(AuthorID) of
        ok ->
            {ok, ok};
        {error, author_not_found} ->
            woody_error:raise(business, #domain_conf_v2_AuthorNotFound{});
        {error, Reason} ->
            woody_error:raise(system, {internal, Reason})
    end.
