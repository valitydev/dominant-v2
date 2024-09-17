%% File: ./dmt_v2/src/dmt_v2_user_op_handler.erl

-module(dmt_v2_user_op_handler).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

%% API
-export([handle_function/4]).

handle_function(Function, Args, WoodyContext0, Options) ->
    DefaultDeadline = woody_deadline:from_timeout(default_handling_timeout(Options)),
    WoodyContext = dmt_v2_api_woody_utils:ensure_woody_deadline_set(WoodyContext0, DefaultDeadline),
    do_handle_function(Function, Args, WoodyContext, Options).

default_handling_timeout(#{default_handling_timeout := Timeout}) ->
    Timeout.

%% Implement the Create function
do_handle_function('Create', {Params}, _Context, _Options) ->
    #domain_conf_v2_UserOpParams{email = Email, name = Name} = Params,
    %% Generate a new UUID for UserOpID
    UserOpID = generate_uuid(#{
        email => Email,
        name => Name
    }),
    %% Insert into op_user table
    case dmt_v2_user_op:insert_user(UserOpID, Name, Email) of
        ok ->
            {ok, #domain_conf_v2_UserOp{id = UserOpID, email = Email, name = Name}};
        {error, already_exists} ->
            {ok, #domain_conf_v2_UserOp{id = UserOpID, email = Email, name = Name}};
        {error, Reason} ->
            woody_error:raise(system, {internal, Reason})
    end;
do_handle_function('Get', {UserOpID}, _Context, _Options) ->
    case dmt_v2_user_op:get_user(UserOpID) of
        {ok, #domain_conf_v2_UserOp{id = ID, email = Email, name = Name}} ->
            {ok, #domain_conf_v2_UserOp{id = ID, email = Email, name = Name}};
        {error, user_not_found} ->
            woody_error:raise(business, #domain_conf_v2_UserOpNotFound{});
        {error, Reason} ->
            woody_error:raise(system, {internal, Reason})
    end;
do_handle_function('Delete', {UserOpID}, _Context, _Options) ->
    case dmt_v2_user_op:delete_user(UserOpID) of
        ok ->
            ok;
        {error, user_not_found} ->
            woody_error:raise(business, #domain_conf_v2_UserOpNotFound{});
        {error, Reason} ->
            woody_error:raise(system, {internal, Reason})
    end.

%% Helper Functions

generate_uuid(Data) ->
    %% Use a UUID generation library or function
    uuid:get_v5(jsx:encode(Data)).
