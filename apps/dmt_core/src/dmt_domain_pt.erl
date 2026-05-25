-module(dmt_domain_pt).

-export([parse_transform/2]).

-spec parse_transform(Forms, term()) -> Forms when
    Forms :: [erl_parse:abstract_form() | erl_parse:form_info()].
parse_transform(Forms, _Options) ->
    [
        erl_syntax:revert(FormNext)
     || Form <- Forms,
        FormNext <- [erl_syntax_lib:map(fun transform/1, Form)],
        FormNext /= delete
    ].

transform(Form) ->
    case erl_syntax:type(Form) of
        function ->
            Name = erl_syntax:concrete(erl_syntax:function_name(Form)),
            Arity = erl_syntax:function_arity(Form),
            transform_function(Name, Arity, Form);
        _ ->
            Form
    end.

transform_function(is_reference_type = Name, 1, FormWas) ->
    % NOTE
    % Replacing `dmt_domain:is_reference_type/1` with a code which does something similar to:
    % ```
    % is_reference_type({struct, struct, {dmsl_domain_thrift, 'CategoryRef'}}) -> {true, 'category'};
    % is_reference_type({struct, struct, {dmsl_domain_thrift, 'CurrencyRef'}}) -> {true, 'currency'};
    % ...
    % is_reference_type(_) -> false.
    % ```
    {struct, union, StructInfo} = dmsl_domain_thrift:struct_info('Reference'),
    ok = validate_reference_struct('Reference', StructInfo),
    Clauses =
        [
            erl_syntax:clause(
                [erl_syntax:abstract(Type)],
                none,
                [erl_syntax:abstract({true, Tag})]
            )
         || {_N, _Req, Type, Tag, _Default} <- StructInfo
        ] ++
            [
                erl_syntax:clause(
                    [erl_syntax:underscore()],
                    none,
                    [erl_syntax:abstract(false)]
                )
            ],
    Form = erl_syntax_lib:map(
        fun(F) -> erl_syntax:copy_attrs(FormWas, F) end,
        erl_syntax:function(
            erl_syntax:abstract(Name),
            Clauses
        )
    ),
    Form;
transform_function(is_reference_type, 2, _FormWas) ->
    % NOTE
    % We need to make `is_reference_type/2` disappear, otherwise it will trigger _unused function_
    % warning.
    delete;
transform_function(_, _, Form) ->
    Form.

validate_reference_struct(StructName, StructInfo) ->
    Mappings = lists:foldl(
        fun({N, _Req, Type, Tag, _Default}, Acc) ->
            maps:put(Type, [{N, Tag} | maps:get(Type, Acc, [])], Acc)
        end,
        #{},
        StructInfo
    ),
    case maps:filter(fun(_, Tags) -> length(Tags) > 1 end, Mappings) of
        Dups when map_size(Dups) > 0 ->
            ErrorMessage = format(
                "struct_info(~0p): multiple fields share the same reference type, "
                "this breaks referential integrity validation",
                [StructName]
            ),
            exit({ErrorMessage, Dups});
        _ ->
            ok
    end.

format(Fmt, Args) ->
    unicode:characters_to_nfc_list(io_lib:format(Fmt, Args)).
