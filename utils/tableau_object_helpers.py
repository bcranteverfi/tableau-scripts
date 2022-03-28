import json
import os
from collections import defaultdict, deque
import tableauserverclient as TSC

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


#
# TSC Metadata Dict Helpers
#
def get_tableau_metadata_dict(auth, server):
    #
    # Get Each Tableau Server Object
    #
    project_dict = get_projects(auth, server)
    wb_dict = get_workbooks(auth, server)
    ds_dict = get_datasources(auth, server)

    #
    # Combine Tableau Server Object Metadata
    #
    meta_dict = combine_object_metadata(project_dict, wb_dict, ds_dict)

    # Write metadata to json file
    with open('../tableau_metadata.json', 'w') as f:
        json.dump(meta_dict, f)

    return meta_dict


def combine_object_metadata(projects, workbooks, datasources):
    repo_paths_by_last = projects.get('repo_paths_by_last')

    id_name_map = {
        **projects.get('id_name_map'),
        **workbooks.get('id_name_map'),
        **datasources.get('id_name_map')
    }

    child_parent_dict = {
        **projects.get('child_parent_dict'),
        **workbooks.get('child_parent_dict'),
        **datasources.get('child_parent_dict')
    }

    return dict(
        repo_paths_by_last=repo_paths_by_last,
        id_name_map=id_name_map,
        child_parent_dict=child_parent_dict
    )


def get_datasources(tsc_auth, tsc_server):

    # Log in to Tableau tsc_server and query all Data Sources on tsc_server
    with tsc_server.auth.sign_in_with_personal_access_token(tsc_auth):
        all_datasources, pagination_item = tsc_server.datasources.get()
        print('There are {} datasources on site...'.format(pagination_item.total_available))

    # Get all Data Source Metadata
    ds_metadata_dict = get_tsc_obj_metadata(all_datasources)

    # Get Data Source ID to Project Name key value dict
    id_name_map = get_id_name_map(all_datasources)

    # Get list of tuples of each Data Source ID and its Parent's ID and sort by parent
    pairs = get_parent_child_pairs(all_datasources, 'project_id')

    # Get all Parent Nodes and their Children as a list
    nodes, child_parent_dict = get_parent_nodes(pairs)

    return dict(
        datasource_metadata=ds_metadata_dict,
        id_name_map=id_name_map,
        pairs=pairs,
        nodes=nodes,
        child_parent_dict=child_parent_dict
    )


def get_projects(tsc_auth, tsc_server):

    # Log in to Tableau Server and get count of Workbooks
    with tsc_server.auth.sign_in_with_personal_access_token(tsc_auth):
        all_projects, pagination_item = tsc_server.projects.get()
        print('There are {} projects on site...'.format(pagination_item.total_available))

    # Get Project ID to Project Name key value dict
    id_name_map = get_id_name_map(all_projects)

    # Get list of tuples of each Project ID and its Parent's ID and sort by parent
    pairs = get_parent_child_pairs(all_projects, 'parent_id')

    # Get all Parent Nodes and their Children as a list
    nodes, child_parent_dict = get_parent_nodes(pairs)

    # Get depth of nodes in directory tree
    root_depth = 0
    root_depth_list = list([nodes.get('root')])
    nodes.pop('root')

    # List of lists where the index represents the node depth level in the directory
    node_depth_list = recurse_nodes(nodes=nodes, node_depth=root_depth_list, depth=root_depth)

    # List of project paths as project IDs
    repo_paths_list = build_repo_paths(depth_list=node_depth_list, pairs=child_parent_dict)

    # List of human-readable project paths
    readable_paths_list = make_readable(repo_paths_list, id_name_map)
    repo_paths_by_last = get_path_by_last(repo_paths_list)

    # Create empty directories
    make_directories(readable_paths_list)

    return dict(
        id_name_map=id_name_map,
        pairs=pairs,
        nodes=nodes,
        child_parent_dict=child_parent_dict,
        readable_paths_list=readable_paths_list,
        repo_paths_list=repo_paths_list,
        repo_paths_by_last=repo_paths_by_last
    )


def get_workbooks(tsc_auth, tsc_server):

    # Log in to Tableau tsc_server and query all Workbooks on tsc_server
    with tsc_server.auth.sign_in_with_personal_access_token(tsc_auth):
        all_workbooks, pagination_item = tsc_server.workbooks.get()
        print('There are {} workbooks on site...'.format(pagination_item.total_available))

    # Get all Workbook Metadata
    wb_metadata_dict = get_tsc_obj_metadata(all_workbooks)

    # Get Workbook ID to Project Name key value dict
    id_name_map = get_id_name_map(all_workbooks)

    # Get list of tuples of each Workbook ID and its Parent's ID and sort by parent
    pairs = get_parent_child_pairs(all_workbooks, 'project_id')

    # Get all Parent Nodes and their Children as a list
    nodes, child_parent_dict = get_parent_nodes(pairs)

    return dict(
        workbook_metadata=wb_metadata_dict,
        id_name_map=id_name_map,
        pairs=pairs,
        nodes=nodes,
        child_parent_dict=child_parent_dict
    )


def get_users_by_name(tsc_auth:TSC.TableauAuth, tsc_server:TSC.Server, user_name:str)->list:
  """
    Return list of users from server with matching username

    tsc_auth: TSC Authentication to server
    tsc_server: TSC Server to be searched
    user_name: Tableau username for User

  """
  
    # Log in to Tableau Server and query all Data Sources on Server
  with tsc_server.auth.sign_in_with_personal_access_token(tsc_auth):

# get Tableau user
    req_options_user = TSC.RequestOptions() 
    req_options_user.filter.add(TSC.Filter(
      TSC.RequestOptions.Field.Name,
      TSC.RequestOptions.Operator.Equals,
      user_name
      ))

    users, pagination_item = tsc_server.users.get(req_options_user)
    
    return users 

def get_groups_by_name(tsc_auth:TSC.TableauAuth, tsc_server:TSC.Server, group_name:str)->list:
  """
   Return list of groups from server with matching Group Name
    
    tsc_auth: TSC Authentication to server
    tsc_server: TSC Server to be searched
    group_name: Name of group to search for
  """
  
    # Log in to Tableau Server and query all Data Sources on Server
  with tsc_server.auth.sign_in_with_personal_access_token(tsc_auth):
    req_options_group = TSC.RequestOptions()
    req_options_group.filter.add(TSC.Filter(
        TSC.RequestOptions.Field.Name,
        TSC.RequestOptions.Operator.Equals,
        group_name
      ))
    groups, pagination_item = tsc_server.groups.get(req_options_group)
  return groups 



def add_user_to_group(tsc_auth: TSC.TableauAuth, tsc_server: TSC.Server, tsc_user: TSC.UserItem, tsc_group: TSC.GroupItem):

  """
  Add a user with matching name to group with matching name 
  """
  
  try:
      with tsc_server.auth.sign_in_with_personal_access_token(tsc_auth): 
        print(f'Adding {tsc_user.name} to {tsc_group.name}')
        tsc_server.groups.add_user(tsc_group, tsc_user.id)
  except TSC.server.endpoint.exceptions.ServerResponseError as err:
    print(f'Error: {err.summary}:  {err.detail}')
  
  return tsc_server 