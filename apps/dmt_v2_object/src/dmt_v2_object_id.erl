-module(dmt_v2_object_id).

%% API
-export([is_id_generatable/1]).
-export([id_generator/1]).

is_id_generatable(currency) ->
    false;
is_id_generatable(Type) ->
    case id_generator(Type) of
        {error, entity_not_supported} ->
            false;
        _ ->
            true
    end.

id_generator(category) ->
    "SELECT jsonb_build_object('category', gen_random_uuid()) AS id";
id_generator(_) ->
    undefined.
