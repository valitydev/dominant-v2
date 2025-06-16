-module(dmt_repository_filter_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").

% We modify records in improper way in tests, so we need to suppress dialyzer warnings
-dialyzer({nowarn_function, [test_all_invalid_objects/0, test_mixed_valid_invalid_objects/0]}).

%% Test the filter_search_results/1 function from dmt_repository module

%% Test setup and teardown
setup() ->
    ok.

cleanup(_) ->
    ok.

%% Test instantiator
filter_search_results_test_() ->
    {setup, fun setup/0, fun cleanup/1, [
        fun test_empty_list/0,
        fun test_all_valid_objects/0,
        fun test_all_invalid_objects/0,
        fun test_mixed_valid_invalid_objects/0,
        fun test_single_valid_object/0,
        fun test_single_invalid_object/0,
        fun test_missing_data_field/0
    ]}.

%% Test cases

test_empty_list() ->
    %% Setup: empty list should return empty list
    Input = [],
    Expected = [],

    Result = dmt_repository:filter_search_results(Input),

    ?assertEqual(Expected, Result).

test_all_valid_objects() ->
    %% Create test objects with data
    %%
    CategoryID = #domain_CategoryRef{id = 1},
    TerminalID = #domain_TerminalRef{id = 1},
    ValidCategory =
        {category, #domain_CategoryObject{
            ref = CategoryID,
            data = #domain_Category{
                name = <<"Test Category">>,
                description = <<"Test Description">>
            }
        }},

    ValidTerminal =
        {terminal, #domain_TerminalObject{
            ref = TerminalID,
            data = #domain_Terminal{
                name = <<"Test Terminal">>,
                description = <<"Test Terminal Description">>
            }
        }},

    Objects = [
        #{data => ValidCategory, id => {category, CategoryID}, version => 1},
        #{data => ValidTerminal, id => {terminal, TerminalID}, version => 1}
    ],

    Result = dmt_repository:filter_search_results(Objects),

    %% All objects should be kept
    ?assertEqual(Objects, Result).

test_all_invalid_objects() ->
    %% Create test objects with invalid data
    CategoryID = #domain_CategoryRef{id = 1},
    TerminalID = #domain_TerminalRef{id = 1},

    InvalidCategory =
        {category, #domain_CategoryObject{
            ref = CategoryID,
            data = #domain_Category{
                % Invalid - missing name
                name = undefined,
                description = <<"Test Description">>
            }
        }},

    InvalidTerminal =
        {terminal_invalid, #domain_TerminalObject{
            ref = TerminalID,
            data = #domain_Terminal{
                % Invalid - missing name
                name = undefined,
                description = <<"Test Terminal Description">>
            }
        }},

    Objects = [
        #{data => InvalidCategory, id => {category, CategoryID}, version => 1},
        #{data => InvalidTerminal, id => {terminal_invalid, TerminalID}, version => 1}
    ],

    Result = dmt_repository:filter_search_results(Objects),

    %% No objects should be kept
    ?assertEqual([], Result).

test_mixed_valid_invalid_objects() ->
    %% Create test objects with mixed valid and invalid data
    CategoryID1 = #domain_CategoryRef{id = 1},
    CategoryID2 = #domain_CategoryRef{id = 2},
    TerminalID = #domain_TerminalRef{id = 1},

    ValidCategory =
        {category, #domain_CategoryObject{
            ref = CategoryID1,
            data = #domain_Category{
                name = <<"Valid Category">>,
                description = <<"Valid Description">>
            }
        }},

    InvalidCategory =
        {category, #domain_CategoryObject{
            ref = CategoryID2,
            data = #domain_Category{
                name = undefined,
                description = <<"Invalid Description">>
            }
        }},

    ValidTerminal =
        {terminal, #domain_TerminalObject{
            ref = TerminalID,
            data = #domain_Terminal{
                name = <<"Valid Terminal">>,
                description = <<"Valid Terminal Description">>
            }
        }},

    Objects = [
        #{data => ValidCategory, id => {category, CategoryID1}, version => 1},
        #{data => InvalidCategory, id => {category, CategoryID2}, version => 1},
        #{data => ValidTerminal, id => {terminal, TerminalID}, version => 1}
    ],

    Result = dmt_repository:filter_search_results(Objects),

    %% Only valid objects should be kept
    Expected = [
        #{data => ValidCategory, id => {category, CategoryID1}, version => 1},
        #{data => ValidTerminal, id => {terminal, TerminalID}, version => 1}
    ],
    ?assertEqual(Expected, Result).

test_single_valid_object() ->
    %% Create test object with valid data
    CategoryID = #domain_CategoryRef{id = 1},

    ValidCategory =
        {category, #domain_CategoryObject{
            ref = CategoryID,
            data = #domain_Category{
                name = <<"Single Valid Category">>,
                description = <<"Single Valid Description">>
            }
        }},

    Objects = [#{data => ValidCategory, id => {category, CategoryID}, version => 1}],

    Result = dmt_repository:filter_search_results(Objects),

    %% Object should be kept
    ?assertEqual(Objects, Result).

test_single_invalid_object() ->
    %% Create test object with invalid data
    CategoryID = #domain_CategoryRef{id = 1},

    InvalidCategory =
        {category_invalid, #domain_CategoryObject{
            ref = CategoryID,
            data = #domain_Category{
                name = <<"Category">>,
                % Too long description
                description = <<"x">>
            }
        }},

    Objects = [#{data => InvalidCategory, id => {category_invalid, CategoryID}, version => 1}],

    Result = dmt_repository:filter_search_results(Objects),

    %% No objects should be kept
    ?assertEqual([], Result).

test_missing_data_field() ->
    %% Test objects without data field - should cause function to crash

    CategoryRef = {category, #domain_CategoryRef{id = 1}},
    % Missing data field
    Objects = [#{id => CategoryRef, version => 1}],

    %% This should crash with badkey error when trying to access data
    ?assertError({badkey, data}, dmt_repository:filter_search_results(Objects)).
