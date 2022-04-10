import pandas as pd


#Function for turning BigQuery rows into dataframes.
#The tuple_list parameter is a list of enumerated columns (tuples) of the initial query.
#It helps us to identify the relationship between result rows and columns.
def row_to_df(row, tuple_list):
    df = pd.DataFrame()
    
    for enum, category in tuple_list:
        category_name = str(category)
        category = []
        category.append(row[enum])        
        df[category_name] = category
        
    return df


#Function for dynamically creating other functions
#The idea comes fromm here https://stackoverflow.com/questions/3687682/how-to-dynamically-define-functions
#In this case, we used it in order to retrieve the keys (subcategories) of dictionaries (categories), that
#are stored as values of the dataframe
def master_subcategory_function(subcat):
    def get_subcat(x):
        try:
            return x.get(subcat)
        except:
            pass
    return get_subcat