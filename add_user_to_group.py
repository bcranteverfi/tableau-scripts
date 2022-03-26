from utils.authentication import authenticate_tableau, Environment
import tableauserverclient as TSC
import csv


def add_user_to_group(user_name: str, group_name:str):

  """
  """
  AUTHENTICATION, SERVER = authenticate_tableau(Environment.PROD)
  
    # Log in to Tableau Server and query all Data Sources on Server
  with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):

# get Tableau user
    req_options_user = TSC.RequestOptions() 
    req_options_user.filter.add(TSC.Filter(
      TSC.RequestOptions.Field.Name,
      TSC.RequestOptions.Operator.Equals,
      user_name
    ))

    users, pagination_item = SERVER.users.get(req_options_user)
    
    num_users_matched = len(users)

    req_options_group = TSC.RequestOptions()
    req_options_group.filter.add(TSC.Filter(
      TSC.RequestOptions.Field.Name,
      TSC.RequestOptions.Operator.Equals,
      group_name
    ))

    groups, pagination_item = SERVER.groups.get(req_options_group)
    
    num_groups_matched = len(groups)

    if num_users_matched == 1 and num_groups_matched==1:
      group = groups[0]
      user = users[0]

      try: 
        print(f'Adding {user.name} to {group.name}')
        SERVER.groups.add_user(group, user.id)
      except TSC.server.endpoint.exceptions.ServerResponseError as err:
        print(f'Error: {err.summary}:  {err.detail}')
    
    else: 
      print(f'unable to update: {num_groups_matched} groups and {num_users_matched} users matching inputs')

    return


def update_user_groups():
 
  with open('user_groups.csv') as file:
    csvreader = csv.reader(file)
    for line in csvreader:

      username = line[0]
      groupname = line[1]
      print(f'user: {username} group: {groupname}')

      add_user_to_group(username, groupname)




if __name__ =='__main__':
  update_user_groups()