from urllib.parse import unquote_plus
from utils import error
import ujson

ds_types = (
    'village',
    'player',
    'tribe'
)

player_stats = [
    "id",
    "villages",
    "points",
    "all_points",
    "att_bash",
    "def_bash",
    "sup_bash",
    "all_bash"
]

tribe_stats = [
    "id",
    "member",
    "villages",
    "points",
    "all_points",
    "att_bash",
    "def_bash",
    "sup_bash",
    "all_bash"
]

stat_shortcuts = {
    'attack': "att_bash",
    'defense': "def_bash",
    'support': "sup_bash",
    'bash': "all_bash"
}


def verify_arguments(**kwargs):
    changed_arguments = []

    order = kwargs.get('order', 'DESC')
    if order.upper() not in ('ASC', 'DESC'):
        raise error.InvalidArgument('order', order)

    amount = kwargs.get('amount', 1)
    if 0 >= amount > 500:
        raise error.InvalidArgument('amount', amount)

    ds_type = kwargs.get('ds_type', 'player')
    if ds_type not in ds_types:
        raise error.InvalidArgument('ds_type', ds_type)

    if (user_tribe_attribute := kwargs.get('tribe_attribute')) is not None:
        real_tribe_attribute = stat_shortcuts.get(user_tribe_attribute, user_tribe_attribute)

        if real_tribe_attribute not in tribe_stats:
            raise error.InvalidArgument('attribute', real_tribe_attribute)
        else:
            changed_arguments.append(real_tribe_attribute)

    if (user_player_attribute := kwargs.get('player_attribute')) is not None:
        real_player_attribute = stat_shortcuts.get(user_player_attribute, user_player_attribute)

        if real_player_attribute not in player_stats:
            raise error.InvalidArgument('attribute', real_player_attribute)
        else:
            changed_arguments.append(real_player_attribute)

    if len(changed_arguments) == 1:
        return changed_arguments[0]
    else:
        return changed_arguments


def parse_result(data, *keys, iterable=False, u_json=False):
    if data is None:
        return data

    if iterable is False:
        data = [data]

    if isinstance(data, list):
        for element in data:
            for key in keys:
                element[key] = unquote_plus(element[key])

    elif isinstance(data, dict):
        for element in data.values():
            for key in keys:
                element[key] = unquote_plus(element[key])

    if iterable and u_json:
        return ujson.dumps(data)
    elif iterable:
        return data
    else:
        return data[0]
