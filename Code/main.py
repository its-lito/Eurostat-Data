import eurostat
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine


# function to process only the necessary data
def filt_df(df):
    df = df.rename(columns={'geo\\time': 'country'})
    df.drop(['c_resid', 'unit', 'nace_r2'], axis='columns', inplace=True)  # drops the unnecessary columns

    df = df.melt(id_vars="country", var_name="year", value_name="value")  # pivots the dataframe
    return df


nights_dataset_df = eurostat.get_data_df(code='tour_occ_ninat')  # download an Eurostat dataset of given code and return
# it as a pandas dataframe.

# using the codes used by eurostat to select the necessary data
total_nights = nights_dataset_df[nights_dataset_df['c_resid'].isin(['TOTAL'])
                                 & nights_dataset_df['unit'].isin(['NR'])
                                 & nights_dataset_df['nace_r2'].isin(['I551-I553'])
                                 & nights_dataset_df['geo\\time'].isin(['EL', 'PT'])].iloc[:, [0, 1, 2, 3, 6, 7, 8, 9]]

total_nights = filt_df(total_nights)
print(total_nights)

foreigns_nights = nights_dataset_df[nights_dataset_df['c_resid'].isin(['FOR'])
                                    & nights_dataset_df['unit'].isin(['NR'])
                                    & nights_dataset_df['nace_r2'].isin(['I551-I553'])
                                    & nights_dataset_df['geo\\time'].isin(['EL', 'PT'])].iloc[:,
                  [0, 1, 2, 3, 6, 7, 8, 9]]

foreigns_nights = filt_df(foreigns_nights)
print('------------------------------')
print(foreigns_nights)

arrivals_dataset_df = eurostat.get_data_df(code='tour_occ_arnat')

total_arrives = arrivals_dataset_df[arrivals_dataset_df['c_resid'].isin(['TOTAL'])
                                    & arrivals_dataset_df['unit'].isin(['NR'])
                                    & arrivals_dataset_df['nace_r2'].isin(['I551-I553'])
                                    & arrivals_dataset_df['geo\\time'].isin(['EL', 'PT'])].iloc[:,
                [0, 1, 2, 3, 6, 7, 8, 9]]

total_arrives = filt_df(total_arrives)
print('------------------------------')
print(total_arrives)

foreigns_arrivals = arrivals_dataset_df[arrivals_dataset_df['c_resid'].isin(['FOR'])
                                        & arrivals_dataset_df['unit'].isin(['NR'])
                                        & arrivals_dataset_df['nace_r2'].isin(['I551-I553'])
                                        & arrivals_dataset_df['geo\\time'].isin(['EL', 'PT'])].iloc[:,
                    [0, 1, 2, 3, 6, 7, 8, 9]]

foreigns_arrivals = filt_df(foreigns_arrivals)
print('------------------------------')
print(foreigns_arrivals)


# plot of the data for Greece,Portugal

def plot(df):
    el_df = df[df.country == 'EL']  # df with data from greece
    es_df = df[df.country == 'PT']  # df with data from portugal

    plt.plot(el_df.year, el_df.value / 10 ** 6)
    plt.plot(es_df.year, es_df.value / 10 ** 6)
    plt.legend(['Greece', 'Portugal'])
    plt.xlabel('Year')
    plt.ylabel('Values')
    return df


total_nights = plot(total_nights)
plt.title("Nights spent at tourist accommodation establishments")
plt.figure()

foreigns_nights = plot(foreigns_nights)
plt.title("Nights spent by non-residents at tourist accommodation establishments")
plt.figure()

total_arrives = plot(total_arrives)
plt.title("Arrivals at tourist accommodation establishments")
plt.figure()

foreigns_arrivals = plot(foreigns_arrivals)
plt.title("Arrivals of non-residents at tourist accommodation establishments")

plt.show()  # dislpays all figures

# Export to csv files

# total_nights.to_csv('Nights spent at tourist accommodation establishments.csv', index=False)
# foreigns_nights.to_csv('Nights spent by non-residents at tourist accommodation establishments.csv', index=False)
# total_arrives.to_csv('Arrivals at tourist accommodation establishments.csv', index=False)
# foreigns_arrivals.to_csv('Arrivals of non-residents at tourist accommodation establishments.csv', index=False)

# Create sqlalchemy engine to connect to localhost database

engine = create_engine("mysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="3457",
                               db="countries"))

# Insert DataFrame to MySQL
total_nights.to_sql('tot_nights', con=engine, if_exists='append', index=False)
foreigns_nights.to_sql('foreign_nights', con=engine, if_exists='append', index=False)

total_arrives.to_sql('tot_arrives', con=engine, if_exists='append', index=False)
foreigns_arrivals.to_sql('foreign_arrives', con=engine, if_exists='append', index=False)

print("Data loaded successfully")
