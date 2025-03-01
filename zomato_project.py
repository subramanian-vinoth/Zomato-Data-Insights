import streamlit as st
import mysql.connector
import pandas as pd

class Database:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.connect()

    def connect(self):
        self.conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.cursor = self.conn.cursor()

    def query(self, sql_query):
        self.cursor.execute(sql_query)
        results = self.cursor.fetchall()
        columns = [col[0] for col in self.cursor.description]
        return pd.DataFrame(results, columns=columns)

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

# Configuration
config = {"host": "localhost", "user": "root", "password": "root", "database": "zomato"}
db = Database(**config)


queries = {"Select": None,
    "Total orders placed by each customer": "SELECT customer_id, COUNT(order_id) AS total_orders FROM orders_table GROUP BY customer_id;",
    "Total revenue from restaurant id": "SELECT restaurant_id, SUM(total_amount) AS total_revenue FROM zomato.orders_table GROUP BY restaurant_id;",
    "Count premium customers": "SELECT COUNT(*) AS premium_customers FROM customer_table WHERE is_premium = 'Yes';",
    "Total deliveries by each delivery person": "SELECT delivery_person_id, COUNT(delivery_id) AS total_deliveries FROM deliveries_table GROUP BY delivery_person_id;",
    "Total revenue generated from orders": "SELECT COUNT(order_id) AS total_order, SUM(total_amount) AS total_revenue FROM orders_table;",
    "Count of orders by status": "SELECT status, COUNT(*) AS order_count FROM orders_table GROUP BY status;",
    "Top 5 restaurants by total orders": "SELECT restaurant_id, COUNT(order_id) AS total_orders FROM orders_table GROUP BY restaurant_id ORDER BY total_orders DESC LIMIT 5;",
    "Customers who have placed more than 2 orders": "SELECT customer_id, COUNT(order_id) AS total_orders FROM orders_table GROUP BY customer_id HAVING total_orders > 2;",
    "Count of deliveries by delivery status": "SELECT delivery_status, COUNT(*) AS delivery_count FROM deliveries_table GROUP BY delivery_status;",
    "Customers who rated their orders below 3": "SELECT customer_id, order_id, feedback_rating FROM orders_table WHERE feedback_rating < 3;",
    "Total number of active restaurants": "SELECT COUNT(*) AS active_restaurants FROM restaurants_table WHERE is_active = 'Yes';",
    "Total discounts applied in orders": "SELECT SUM(discount_applied) AS total_discounts FROM orders_table;",
    "Delivery persons with average rating above 4": "SELECT delivery_person_id, name FROM delivery_persons_table WHERE average_rating > 4;",
    "Most popular cuisine type based on total orders": "SELECT r.cuisine_type, COUNT(o.order_id) AS total_orders FROM orders_table o JOIN restaurants_table r ON o.restaurant_id = r.restaurant_id GROUP BY r.cuisine_type ORDER BY total_orders DESC LIMIT 1;",
    "Delivery time comparison between vehicle types": "SELECT vehicle_type, AVG(delivery_time) AS avg_delivery_time FROM deliveries_table GROUP BY vehicle_type;",
    "Average Delivery Fee by Vehicle Type": "SELECT vehicle_type, AVG(delivery_fee) AS avg_delivery_fee FROM deliveries_table GROUP BY vehicle_type;",
    "Top 5 Restaurants by Average Rating": "SELECT restaurant_id, AVG(rating) AS avg_rating FROM restaurants_table GROUP BY restaurant_id ORDER BY avg_rating DESC LIMIT 5;",
    "Count of Orders by Cuisine Type": "SELECT r.cuisine_type, COUNT(o.order_id) AS order_count FROM orders_table o JOIN restaurants_table r ON o.restaurant_id = r.restaurant_id GROUP BY r.cuisine_type;",
    "Total Deliveries by Vehicle Type": "SELECT vehicle_type, COUNT(delivery_id) AS total_deliveries FROM deliveries_table GROUP BY vehicle_type;",
    "Delivery persons who have completed deliveries within 30 minutes": "SELECT dp.delivery_person_id, dp.name, COUNT(d.delivery_id) AS deliveries_within_30_min FROM deliveries_table d JOIN delivery_persons_table dp ON d.delivery_person_id = dp.delivery_person_id WHERE d.delivery_time <= 30 GROUP BY dp.delivery_person_id, dp.name;",
    "Identifying Peak Ordering Times": """SELECT 
    CASE 
        WHEN HOUR(order_date) >= 0 AND HOUR(order_date) < 4 THEN '00-04'
        WHEN HOUR(order_date) >= 4 AND HOUR(order_date) < 8 THEN '04-08'
        WHEN HOUR(order_date) >= 8 AND HOUR(order_date) < 12 THEN '08-12'
        WHEN HOUR(order_date) >= 12 AND HOUR(order_date) < 16 THEN '12-16'
        WHEN HOUR(order_date) >= 16 AND HOUR(order_date) < 20 THEN '16-20'
        WHEN HOUR(order_date) >= 20 AND HOUR(order_date) < 24 THEN '20-24'
    END AS hour_range,
    COUNT(*) AS no_of_orders 
    FROM 
        orders_table 
    GROUP BY 
        hour_range 
    ORDER BY 
        hour_range;"""
}



# Sidebar for navigation
st.sidebar.title("MAIN MENU")
page = st.sidebar.radio("Select a page:", ["Home", "Data Exploration"])

# Update the session state based on sidebar selection
if page == "Home":
    st.session_state['page'] = "home"
elif page == "Data Exploration":
    st.session_state['page'] = "data_exploration"

# Home Page
if st.session_state['page'] == "home":
    st.title("Welcome to Zomato Data Explorer")
    st.write("Explore various data related to Zomato, including customer information, order statistics, and more!")

# Data Exploration Page
if st.session_state['page'] == "data_exploration":
    selected_table = st.selectbox("Select a table:", ["Select","customer_table", "deliveries_table", "delivery_persons_table", "orders_table", "restaurants_table"])  # Example table names
    selected_query = st.selectbox("Select a query:", list(queries.keys()))  # Example queries

    if selected_table and selected_table != "Select":
        query = f"SELECT * FROM {selected_table};"
        table_data = db.query(query)  # Ensure `db` is defined and connected
        st.write(f"Data from {selected_table}:")
        st.dataframe(table_data)

    if selected_query and selected_query != "Select":
        query_result = db.query(queries[selected_query])  # Ensure `queries` is defined
        st.write(f"Results for: {selected_query}")
        st.dataframe(query_result)
    
# Close the database connection when done
db.close_connection()
