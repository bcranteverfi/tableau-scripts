import os
from collections import defaultdict, deque


#
# TSC Object Helpers
#
# Wrangle Tableau Server Client Python library
# objects into usable Python data structures.
#

def get_tsc_obj_path(tsc_obj, metadata):
    id_map = metadata.get('id_name_map')
    tsc_obj_path_ids = metadata.get('repo_paths_by_last').get(tsc_obj.project_id or 'root')
    tsc_obj_path_readable = [id_map.get(p) or 'root' for p in tsc_obj_path_ids]
    tsc_obj_path = os.path.join('..', os.path.join('', *tsc_obj_path_readable))

    # Ensure path to download files exists, create if not
    if not os.path.exists(tsc_obj_path):
        os.mkdir(tsc_obj_path)

    return tsc_obj_path


def get_path_by_last(paths_list):
    repo_paths_by_last = defaultdict(list)
    for path_list in paths_list:
        repo_paths_by_last[path_list[-1]] = path_list

    return repo_paths_by_last


def get_tsc_obj_metadata(tsc_obj_list):
    tsc_obj_metadata = defaultdict(dict)
    for tsc_obj in tsc_obj_list:
        tsc_obj_metadata[tsc_obj.id] = vars(tsc_obj)

    return tsc_obj_metadata


def get_id_name_map(tsc_objects):
    key_value_dict = dict()
    for item in tsc_objects:
        key_value_dict[item.id] = item.name.replace('/', '|')

    return key_value_dict


def get_parent_child_pairs(tsc_objects_list, parent_field):
    # Get list of tuples of each Object ID and its Parent's ID. Sort by parent
    parent_child_pairs = [
        tuple((str(vars(i).get(parent_field) or vars(i).get('_' + parent_field) or 'root'), i.id))
        for i in tsc_objects_list
    ]

    parent_child_pairs.sort(key=lambda x: x[0])

    return parent_child_pairs


def get_parent_nodes(parent_child_pairs):
    # Get all Parent Nodes and their Children as a list
    parent_nodes = defaultdict(list)
    child_parent_relation = dict()
    for parent, child in parent_child_pairs:
        parent_nodes[parent].append(child)
        child_parent_relation[child] = parent

    return parent_nodes, child_parent_relation


#
# TSC Helpers - Projects (Directory Structure)
#
def make_directories(paths_to_make):
    for p in paths_to_make:
        root_path = os.path.join('..', p)
        if not os.path.exists(root_path):
            os.makedirs(root_path)

    return


def make_readable(path_lists, id_map):
    readable_lists = list()
    for path_list in path_lists:
        readable_list = [id_map.get(p) or 'root' for p in path_list]
        readable_lists.append(
            os.path.join('', *readable_list)
        )

    return readable_lists


def build_repo_paths(depth_list=None, pairs=None):
    repo_paths = list()

    def get_all_parents(child_node, parent_deque):
        parent_deque.appendleft(child_node)
        if pairs.get(child_node) is not None:
            get_all_parents(pairs.get(child_node), parent_deque)
        else:
            return repo_paths.append(list(parent_deque))

    for idx in range(0, len(depth_list)):
        for child in depth_list[idx]:
            parent_tree = deque()
            get_all_parents(child, parent_tree)

    return repo_paths


def recurse_nodes(nodes=None, node_depth=None, depth=None):
    next_depth = list()
    _nodes = nodes.copy()
    for p, c in nodes.items():
        if p in node_depth[depth]:
            next_depth.extend(c)
            del _nodes[p]

    if bool(nodes):
        depth += 1
        node_depth.append(next_depth)
        recurse_nodes(
            nodes=_nodes,
            node_depth=node_depth,
            depth=depth
        )

    return node_depth
