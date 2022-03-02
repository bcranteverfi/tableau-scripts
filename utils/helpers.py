import json


def log(var_name, var):
    return print('{:32s} {:6s} \t {}'.format(str(var_name), str(type(var)), str(var)))


def pp(json_dict):
    return print(json.dumps(json_dict, indent=4, sort_keys=True))


def table_name_fmt(name):
    return name.replace('-', '_')


def strip_brackets(field_name):
    return field_name.replace('[', '').replace(']', '')


def denormalize_json(nested_dict):
    flat_dict = {}
    nested_dicts = {}
    for key in nested_dict.keys():
        if type(nested_dict.get(key)) != dict:
            flat_dict[key] = nested_dict.get(key)
        else:
            for nested_key in nested_dict.get(key).keys():
                nested_dicts[key + '.' + nested_key] = nested_dict.get(key).get(nested_key)

    return {**nested_dicts, **flat_dict}
