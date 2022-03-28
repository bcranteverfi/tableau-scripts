from utils.authentication import authenticate_tableau, Environment
from utils.tableau_object_helpers import (
    get_users_by_name,
    get_groups_by_name,
    add_user_to_group,
)
import tableauserverclient as TSC
import csv

# Usage:
#   This module expects a .csv in the same directory table "user_groups.csv"
#   Create a .csv with two colums and save to TABLEAU-SCRIPTS directory
#        Column 1: User name for user being added to group
#        Column 2: Name of group user will be added to


def update_user_groups():
    AUTHENTICATION, SERVER = authenticate_tableau(Environment.DEV)
    with open("user_groups.csv") as file:
        csvreader = csv.reader(file)
        for line in csvreader:

            username = line[0]
            groupname = line[1]

            tableau_users = get_users_by_name(AUTHENTICATION, SERVER, username)
            tableau_groups = get_groups_by_name(AUTHENTICATION, SERVER, groupname)
            print(tableau_users)
            if len(tableau_groups) == 1 and len(tableau_users) == 1:
                add_user_to_group(
                    AUTHENTICATION, SERVER, tableau_users[0], tableau_groups[0]
                )
            else:
                print(
                    f"Unable to update: {len(tableau_users)} users and {len(tableau_groups)}"
                )


if __name__ == "__main__":
    update_user_groups()
