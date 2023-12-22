import streamlit as st
import pandas as pd
import pickle
import matplotlib.pyplot as plt

st.set_page_config(
    page_title='Credit Card Default - EDA',
    layout='wide',
    initial_sidebar_state='expanded'
)

# Load dump files
with open('annual_model.pkl', 'rb') as file_1:
    annual_model = pickle.load(file_1)

with open('monhtly_model.pkl', 'rb') as file_1:
    monthly_model = pickle.load(file_1)    

# Read CSV into dataframe
df = pd.read_csv('cleaned_data.csv')
df = df.drop(columns=['Unnamed: 0'])

def run():
    # Creating title
    st.title('Subscription Revenue Forecast')
    
    # Menambahkan deskripsi
    st.write('Page written and coded by Audrey Wanto, Jeni Kasturi, Taufiqurrahman')
    st.write('Batch: BSD 002')
    st.write('Group 1, Final Project')
   
    st.write('---') 
    
    # Making form
    with st.form(key='form parameters'):
        sub_type = st.selectbox('Subscription Type', ('Annual Subscription', 'Monthly Subscription'))
        month = st.slider('Month of Year', 1, 12, 12, help='1 For January, 12 for December')
        year = st.selectbox('Year of Forecast', (2022, 2023), help='Further years are excluded as prediction may not be accurate enough.')

        st.markdown('---')
        
        submitted = st.form_submit_button('Predict')
    
    # Set variables
    period = 0
    value = 0
    model = 0
    
    # Conditional for period forecasting
    if year == 2022:
        period = month
    else:
        period = month + 12
        
    # Conditional for value multiplier in calculation of revenue
    if sub_type == 'Annual Subscription':
        value = 1200
        model = annual_model
    else:
        value = 125
        model = monthly_model
        
    # Groupby amount of customers
    groupby = df.groupby([pd.Grouper(key='signup_date_time', freq='MS')])['name'].value_counts().reset_index(name='customers')
    groupby.columns = ['Signup Date', 'Type', 'Number of Customers']
    pivot = groupby.pivot(values='Number of Customers', index= 'Signup Date', columns = 'Type')
    pivot.columns = ['Annual Subscription', 'Monthly Subscription']
    pivot.index.freq = 'MS'
    
    if submitted:
        # Getting a forecast for the next [x] periods and extracting summary statistics
        result = model.get_forecast(period).summary_frame()

        # Plotting the acttual values against the forecasted values
        pivot[sub_type].plot(label='Actual') # Plotting actual data
        result['mean'].plot(label='Forecast') # Plotting forecasted mean values
        plt.fill_between(result.index, result['mean_ci_lower'], result['mean_ci_upper'], color='k', alpha=0.1, label='Bound')
        
        # Filling area between upper and lower confidence intervals for the forecast
        fig = plt.figure(figsize=(15,5))
        plt.legend(loc='upper left')  # Adding legend to the plot
        plt.title('Annual Subscription (MAE:{})'.format(round(model.mae, 2)))  # Setting the plot title
        st.pyplot(fig)  # Displaying the plot
        
        # Printing the forecasted number of new customers for the annual subscription in 2022
        st.write('## Forecast of New Customers (', sub_type, ') in ', year, ':', int(result['mean'].sum()))
        # Printing the forecasted revenue for the annual subscription in 2022
        st.write('## Forecast of Revenue (', sub_type, ') in ', year, ':', int(result['mean'].sum()) * value, '$')
        
        
if __name__== '__main__':
    run()