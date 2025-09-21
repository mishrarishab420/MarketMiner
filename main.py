import streamlit as st
import pandas as pd
import numpy as np
import time
import plotly.express as px
from datetime import datetime
import os
import sys
import threading
from scraping.amazon_scrapper import scrape_amazon

# App Configuration
# ---------------------------
st.set_page_config(
    page_title="Insightify",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Compact CSS with Tighter Spacing
st.markdown("""
    <style>
        :root {
            --primary: #00d4ff;
            --secondary: #ff00e4;
            --bg: #0f0f1a;
            --card-bg: #1a1a2e;
            --text: #e6e6e6;
        }
        
        html, body, [class*="css"] {
            background-color: var(--bg);
            color: var(--text);
            font-family: 'Inter', sans-serif;
        }
        
        .main {
            padding-top: 0.5rem !important;
            margin-bottom: 0rem;
        }
        
        .stApp {
            margin-top: -4rem; 
        }
        
        .sidebar .sidebar-content {
            background-color: var(--card-bg);
            border-right: 1px solid #2a2a3c;
        }
        
        .stButton>button {
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            color: white;
            border: none;
            border-radius: 12px;
            padding: 0.6rem 1.2rem;
            font-weight: 600;
            transition: all 0.3s ease;
            width: 100%;
            margin-top: 0.5rem;
        }
        
        .card {
            background-color: var(--card-bg);
            border-radius: 12px;
            padding: 1.2rem;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
            border: 1px solid #2a2a3c;
            margin-bottom: 1rem !important;
        }
        
        .header-container {
            margin-bottom: 0.5rem !important;
        }
        
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.5rem;
        }
        
        .stTabs [data-baseweb="tab"] {
            padding: 0.5rem 1rem;
            border-radius: 8px;
        }
        
        .dataframe {
            margin: 0.5rem 0 !important;
        }
        
        [data-testid="column"] {
            padding: 0.5rem !important;
        }
        
        [data-testid="stMetric"] {
            padding: 0.5rem !important;
        }
    </style>
""", unsafe_allow_html=True)


# ---------------------------
# Compact Header Section
# ---------------------------
st.markdown("""
    <div class="header-container">
        <h1 style="font-size: 2.5rem; font-weight: 800; 
                   background: linear-gradient(135deg, #00d4ff, #ff00e4);
                   -webkit-background-clip: text;
                   -webkit-text-fill-color: transparent;
                   margin-bottom: 0.25rem;">iNSIGHTIFY</h1>
         <p style="font-size: 1rem; color: #b3b3b3; margin-bottom: 0.5rem;">
            From Clicks to Clarity • AI-Powered Data Insights • Real-Time Analytics
        </p>
    </div>
""", unsafe_allow_html=True)

st.markdown("---")

# Initialize session state for scraper
if 'scraping_in_progress' not in st.session_state:
    st.session_state.scraping_in_progress = False
if 'scraping_complete' not in st.session_state:
    st.session_state.scraping_complete = False
if 'scraped_data' not in st.session_state:
    st.session_state.scraped_data = None
if 'progress' not in st.session_state:
    st.session_state.progress = 0

# Function to run scraper in a thread
# In your run_scraper function
def run_scraper(query, pages):
    try:
        st.session_state.scraping_in_progress = True
        st.session_state.scraping_complete = False
        
        # Create a progress callback function
        def progress_callback(progress):
            st.session_state.progress = progress
        
        # Run the scraper
        df = scrape_amazon(query, pages, progress_callback)
        
        st.session_state.scraped_data = df
        st.session_state.scraping_complete = True
        st.session_state.scraping_in_progress = False
        
    except Exception as e:
        st.error(f"Error during scraping: {str(e)}")
        st.session_state.scraping_in_progress = False

# ---------------------------
# Sidebar Configuration
# ---------------------------
with st.sidebar:
    st.markdown("""
        <p style="font-size: 1.25rem; font-weight: 700; 
                 background: linear-gradient(135deg, #00d4ff, #ff00e4);
                 -webkit-background-clip: text;
                 -webkit-text-fill-color: transparent;
                 margin-bottom: 1rem; margin-top : 4rem;">Data Pipeline</p>
    """, unsafe_allow_html=True)
    
    data_source = st.radio(
        "SELECT DATA SOURCE:",
        ("🌐 Web Scraper", "📁 Upload CSV"),
        index=0
    )
    
    if data_source == "🌐 Web Scraper":
        website = st.selectbox(
            "E-COMMERCE PLATFORM:",
            ["Amazon", "Flipkart", "Meesho", "Ebay", "AliExpress"]
        )
        
        query = st.text_input(
            "SEARCH QUERY:",
            placeholder="e.g. iPhone 15, Smartwatch, T-shirt"
        )
        
        if website == "Amazon":
            num_pages = st.number_input(
                "Number of Pages to Scrape:",
                min_value=1,
                max_value=20,
                step=1
            )
        else:
            num_pages = 1
        
        if st.button("🚀 LAUNCH SCRAPER", type="primary") and query:
            if not st.session_state.scraping_in_progress:
                # Start scraping in a separate thread
                thread = threading.Thread(target=run_scraper, args=(query, num_pages))
                thread.start()
    
    else:
        uploaded_file = st.file_uploader(
            "UPLOAD DATASET:",
            type=["csv", "xlsx"]
        )

# ---------------------------
# Main Content Area
# ---------------------------
if st.session_state.scraping_in_progress:
    st.header("Scraping in Progress")
    st.info("Please wait while we scrape data from Amazon. This may take several minutes.")
    
    # Progress bar
    progress_bar = st.progress(st.session_state.progress)
    
    # Update progress bar in a loop while scraping
    while st.session_state.scraping_in_progress and st.session_state.progress < 100:
        time.sleep(0.1)
        progress_bar.progress(st.session_state.progress)
    
    progress_bar.progress(100)

elif st.session_state.scraping_complete and st.session_state.scraped_data is not None:
    st.header("Scraping Complete!")
    st.success("Data has been successfully scraped and saved to the data folder.")
    
    # Display the scraped data
    st.subheader("Preview of Scraped Data")
    st.dataframe(st.session_state.scraped_data.head(10))
    
    # Show basic statistics
    st.subheader("Data Overview")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Products", len(st.session_state.scraped_data))
    with col2:
        st.metric("Columns", len(st.session_state.scraped_data.columns))
    with col3:
        # Check if price column exists
        price_col = None
        for col in st.session_state.scraped_data.columns:
            if 'price' in col.lower() or 'current' in col.lower():
                price_col = col
                break
        
        if price_col:
            try:
                avg_price = st.session_state.scraped_data[price_col].replace('', np.nan).dropna().astype(float).mean()
                st.metric("Average Price", f"₹{avg_price:.2f}")
            except:
                st.metric("Average Price", "N/A")
        else:
            st.metric("Average Price", "N/A")
    
    # Option to download the data
    csv = st.session_state.scraped_data.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"amazon_{query}_products.csv",
        mime="text/csv",
    )

else:
    # Default view when no scraping has been done
    st.header("Welcome to Insightify")
    st.markdown("""
    <div class="card">
        <h3>📊 Data Analytics Platform</h3>
        <p>Use the sidebar to:</p>
        <ul>
            <li>Scrape data from e-commerce platforms</li>
            <li>Upload your own datasets</li>
            <li>Analyze and visualize data</li>
            <li>Generate insights and reports</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    
    # Display recent data files if they exist
    data_dir = "data"
    if os.path.exists(data_dir) and os.listdir(data_dir):
        st.subheader("Recently Scraped Data Files")
        recent_files = []
        for file in os.listdir(data_dir):
            if file.endswith('.csv'):
                file_path = os.path.join(data_dir, file)
                mod_time = os.path.getmtime(file_path)
                recent_files.append((file, mod_time))
        
        # Sort by modification time (newest first)
        recent_files.sort(key=lambda x: x[1], reverse=True)
        
        for file, mod_time in recent_files[:5]:  # Show only 5 most recent
            file_date = datetime.fromtimestamp(mod_time).strftime("%Y-%m-%d %H:%M")
            col1, col2 = st.columns([3, 1])
            with col1:
                st.write(f"📄 {file}")
            with col2:
                st.write(f"_{file_date}_")