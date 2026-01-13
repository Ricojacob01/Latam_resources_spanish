import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd
from datetime import date

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config() # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall_arrow()
            if result:
                return result.to_pandas()
            return pd.DataFrame()

def sqlExecute(query: str):
    """Execute a SQL statement without returning results (for INSERT/UPDATE)"""
    cfg = Config()
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

st.set_page_config(layout="wide")

@st.cache_data(ttl=30)  # only re-query if it's been 30 seconds
def getData():
    return sqlQuery("SELECT * FROM sdp_workshop_rico_martinez.ia.opiniones ORDER BY fecha DESC, id_opinion DESC")

st.header("Opiniones Management System")

# Create tabs for different operations
tab1, tab2, tab3 = st.tabs(["📊 View Data", "➕ Add New Opinion", "✏️ Edit Opinion"])

with tab1:
    st.subheader("All Opinions")
    data = getData()
    st.dataframe(data=data, height=600, use_container_width=True)
    st.info(f"Total records: {len(data)}")

with tab2:
    st.subheader("Add New Opinion")
    with st.form("add_form"):
        col1, col2 = st.columns(2)
        with col1:
            new_fecha = st.date_input("Fecha", value=date.today())
            new_id_opinion = st.number_input("ID Opinion", min_value=1, step=1)
        with col2:
            new_id_cliente = st.number_input("ID Cliente", min_value=1, step=1)
        
        new_opinion = st.text_area("Opinion", height=150)
        
        submitted = st.form_submit_button("Add Opinion")
        if submitted:
            if new_opinion.strip():
                try:
                    # Clear cache to refresh data
                    st.cache_data.clear()
                    
                    insert_query = f"""
                    INSERT INTO sdp_workshop_rico_martinez.ia.opiniones 
                    (fecha, id_opinion, id_cliente, opinion)
                    VALUES ('{new_fecha}', {new_id_opinion}, {new_id_cliente}, '{new_opinion.replace("'", "''")}')
                    """
                    sqlExecute(insert_query)
                    st.success("✅ Opinion added successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error adding opinion: {str(e)}")
            else:
                st.warning("⚠️ Please enter an opinion text.")

with tab3:
    st.subheader("Edit Existing Opinion")
    data = getData()
    
    if len(data) > 0:
        # Create a selection dropdown
        data['display'] = data.apply(lambda row: f"ID {row['id_opinion']} - Cliente {row['id_cliente']} - {row['fecha']}", axis=1)
        selected_display = st.selectbox("Select opinion to edit", data['display'].tolist())
        
        if selected_display:
            selected_row = data[data['display'] == selected_display].iloc[0]
            
            with st.form("edit_form"):
                st.write(f"**Fecha:** {selected_row['fecha']}")
                st.write(f"**ID Opinion:** {selected_row['id_opinion']}")
                st.write(f"**ID Cliente:** {selected_row['id_cliente']}")
                
                st.write("**Current Opinion:**")
                st.info(selected_row['opinion'])
                
                updated_opinion = st.text_area("New Opinion", value=selected_row['opinion'], height=150)
                
                submitted_edit = st.form_submit_button("Update Opinion")
                if submitted_edit:
                    if updated_opinion.strip():
                        try:
                            # Clear cache to refresh data
                            st.cache_data.clear()
                            
                            update_query = f"""
                            UPDATE sdp_workshop_rico_martinez.ia.opiniones 
                            SET opinion = '{updated_opinion.replace("'", "''")}'
                            WHERE id_opinion = {selected_row['id_opinion']} 
                            AND id_cliente = {selected_row['id_cliente']}
                            AND fecha = '{selected_row['fecha']}'
                            """
                            sqlExecute(update_query)
                            st.success("✅ Opinion updated successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ Error updating opinion: {str(e)}")
                    else:
                        st.warning("⚠️ Opinion cannot be empty.")
    else:
        st.info("No opinions available to edit. Add some opinions first!")
