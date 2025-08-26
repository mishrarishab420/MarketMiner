import streamlit as st
import pandas as pd
import numpy as np
import time
import plotly.express as px
from datetime import datetime

# ---------------------------
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
            padding-top: 0.5rem !important;  /* Reduced from 1rem */
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
        
        /* Compact cards */
        .card {
            background-color: var(--card-bg);
            border-radius: 12px;
            padding: 1.2rem;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
            border: 1px solid #2a2a3c;
            margin-bottom: 1rem !important;
        }
        
        /* Tighten header spacing */
        .header-container {
            margin-bottom: 0.5rem !important;
        }
        
        /* Compact tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.5rem;
        }
        
        .stTabs [data-baseweb="tab"] {
            padding: 0.5rem 1rem;
            border-radius: 8px;
        }
        
        /* Compact data tables */
        .dataframe {
            margin: 0.5rem 0 !important;
        }
        
        /* Remove extra padding in columns */
        [data-testid="column"] {
            padding: 0.5rem !important;
        }
        
        /* Tighten metric spacing */
        [data-testid="stMetric"] {
            padding: 0.5rem !important;
        }
    </style>
""", unsafe_allow_html=True)

# ---------------------------
# Sample Data Generator (unchanged)
# ---------------------------
def generate_sample_data(query, website):
    """Generate realistic sample e-commerce data for demonstration"""
    np.random.seed(42)
    
    products = {
        "iPhone": ["iPhone 15 Pro", "iPhone 14", "iPhone SE"],
        "Smartwatch": ["Apple Watch Ultra", "Samsung Galaxy Watch", "Fitbit Sense"],
        "T-shirt": ["Cotton Polo T-Shirt", "Graphic Tee", "Athletic Dry-Fit"]
    }
    
    # Get relevant products based on query
    product_type = next((k for k in products if k.lower() in query.lower()), "iPhone")
    product_list = products.get(product_type, products["iPhone"])
    
    # Generate 10 items (3 full sets of products plus 1 extra)
    num_items = 10
    repeated_products = (product_list * (num_items // len(product_list) + 1))[:num_items]
    
    # Generate data with consistent lengths
    data = pd.DataFrame({
        "Product": [f"{p} ({website})" for p in repeated_products],
        "Price (₹)": np.random.randint(
            10000, 150000, num_items) if product_type == "iPhone" 
            else np.random.randint(5000, 35000, num_items) if product_type == "Smartwatch"
            else np.random.randint(300, 2000, num_items),
        "Rating": np.round(np.random.uniform(3.5, 5, num_items), 1),
        "Reviews": np.random.randint(50, 2000, num_items),
        "Seller": ["Official Store" if x%3==0 else "Premium Reseller" if x%3==1 else "Marketplace Seller" 
                  for x in range(num_items)],
        "In Stock": [True if x%4!=0 else False for x in range(num_items)],
        "Discount %": np.random.randint(0, 40, num_items),
        "Scraped At": [datetime.now().strftime("%Y-%m-%d %H:%M") for _ in range(num_items)]
    })
    
    # Add some outliers
    if len(data) > 3:
        data.loc[3, "Price (₹)"] = data["Price (₹)"].max() * 1.8
        data.loc[7, "Rating"] = data["Rating"].min() * 0.9
    
    return data.sort_values("Price (₹)")

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

# ---------------------------
# Sidebar Configuration (more compact)
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
            value="iPhone 15",
            placeholder="e.g. iPhone 15, Smartwatch, T-shirt"
        )
        
        with st.expander("⚙️ Advanced Settings", expanded=False):
            st.slider("MAX RESULTS:", 10, 500, 50, 10)
            st.checkbox("Include Product Reviews", value=True)
            st.checkbox("Include Product Images", value=False)
        
        if st.button("🚀 LAUNCH SCRAPER", type="primary"):
            st.session_state.run_scraper = True
    
    else:
        uploaded_file = st.file_uploader(
            "UPLOAD DATASET:",
            type=["csv", "xlsx"]
        )

# ---------------------------
# Main Content Area (more compact)
# ---------------------------
if data_source == "🌐 Web Scraper" and st.session_state.get('run_scraper', False):
    # Compact progress animation
    with st.container():
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for percent in range(101):
            time.sleep(0.02)
            progress_bar.progress(percent)
            status_text.text(f"🚀 Scraping {website} for '{query}'... {percent}%")
        
        progress_bar.empty()
        status_text.empty()
        
        # Generate and display sample data
        sample_data = generate_sample_data(query, website)
        
        # Main data card with tabs - more compact
        with st.container():
            st.markdown(f"""
                <div class="card">
                    <h3 style="margin-bottom: 0.5rem;">📊 {website} Results for: "{query}"</h3>
                    <p style="margin-bottom: 0;">{len(sample_data)} products found • Updated: {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>
                </div>
            """, unsafe_allow_html=True)
            
            tab1, tab2, tab3 = st.tabs(["📋 Data Table", "📈 Insights", "📌 Highlights"])
            
            with tab1:
                st.dataframe(
                    sample_data,
                    column_config={
                        "Price (₹)": st.column_config.NumberColumn(format="₹%,d"),
                        "Rating": st.column_config.ProgressColumn(format="%.1f ★", min_value=0, max_value=5),
                        "Discount %": st.column_config.ProgressColumn(format="%d%%", min_value=0, max_value=100)
                    },
                    hide_index=True,
                    use_container_width=True
                )
                
                st.download_button(
                    label="📥 Download Full Dataset",
                    data=sample_data.to_csv(index=False).encode('utf-8'),
                    file_name=f"{website.lower()}_{query.replace(' ', '_')}.csv",
                    mime='text/csv'
                )
            
            with tab2:
                col1, col2 = st.columns(2)
                with col1:
                    # Price distribution
                    fig1 = px.histogram(
                        sample_data, 
                        x="Price (₹)", 
                        nbins=10,
                        title="Price Distribution",
                        color_discrete_sequence=['#00d4ff']
                    )
                    st.plotly_chart(fig1, use_container_width=True)
                
                with col2:
                    # Rating vs Price scatter
                    fig2 = px.scatter(
                        sample_data,
                        x="Price (₹)",
                        y="Rating",
                        color="Seller",
                        size="Reviews",
                        hover_name="Product",
                        title="Price vs Rating by Seller",
                        color_discrete_sequence=['#00d4ff', '#ff00e4', '#00ffaa']
                    )
                    st.plotly_chart(fig2, use_container_width=True)
            
            with tab3:
                # Key metrics in a tight row
                col1, col2, col3 = st.columns(3)
                col1.metric("Avg Price", f"₹{int(sample_data['Price (₹)'].mean()):,}")
                col2.metric("Avg Rating", f"{sample_data['Rating'].mean():.1f} ★")
                col3.metric("Total Reviews", f"{sample_data['Reviews'].sum():,}")
                
                # Best deals with compact layout
                st.markdown("#### 💰 Best Deals")
                best_deals = sample_data.sort_values(by=["Discount %", "Rating"], ascending=[False, False]).head(3)
                st.dataframe(
                    best_deals[["Product", "Price (₹)", "Discount %", "Rating"]],
                    hide_index=True,
                    use_container_width=True,
                    height=150
                )

elif data_source == "📁 Upload CSV" and uploaded_file is not None:
    try:
        if uploaded_file.name.endswith('.csv'):
            data = pd.read_csv(uploaded_file)
        else:
            data = pd.read_excel(uploaded_file)
        
        # Compact data preview
        with st.container():
            st.markdown("""
                <div class="card">
                    <h3 style="margin-bottom: 0.5rem;">📋 Uploaded Dataset Overview</h3>
                </div>
            """, unsafe_allow_html=True)
            
            st.dataframe(data.head(8), use_container_width=True, height=300)
            
    except Exception as e:
        st.error(f"❌ Error processing file: {str(e)}")

else:
    # Welcome section with compact layout
    st.markdown("""
        <div class="card">
            <h3 style="margin-bottom: 0.5rem;">🚀 Welcome to Insightify</h3>
            <p style="margin-bottom: 0;">Transform raw data into actionable insights with our AI-powered analytics platform.</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Feature cards in tight grid
    features = [
        ("🔍", "Smart Scraping", "Extract data from e-commerce sites with intelligent pagination"),
        ("📊", "Visual Analytics", "Interactive dashboards with Plotly charts for exploration"),
        ("🤖", "AI Insights", "Automated trend detection and predictive analytics")
    ]
    
    cols = st.columns(3)
    for i, (icon, title, desc) in enumerate(features):
        with cols[i]:
            st.markdown(f"""
                <div class="card" style="height: 100%;">
                    <h4 style="margin-bottom: 0.25rem;">{icon} {title}</h4>
                    <p style="margin-bottom: 0;">{desc}</p>
                </div>
            """, unsafe_allow_html=True)
    
    # Compact quick start guide
    with st.expander("🎯 Quick Start Demo", expanded=True):
        st.markdown("""
            1. Select **🌐 Web Scraper** in the sidebar  
            2. Choose a platform (e.g. Amazon)  
            3. Enter a product (e.g. "Smartwatch")  
            4. Click **🚀 LAUNCH SCRAPER**  
            
            Or try our sample dataset now:
        """)
        
        if st.button("✨ Load Sample Smartwatch Data", type="primary"):
            sample_data = generate_sample_data("Smartwatch", "Amazon")
            st.session_state.sample_data = sample_data
            st.session_state.show_sample = True
            st.rerun()
    
    if st.session_state.get('show_sample', False):
        st.markdown("---")
        st.markdown("### 📊 Sample Smartwatch Data (Amazon)")
        st.dataframe(st.session_state.sample_data, use_container_width=True, height=300)

# Minimal footer
st.markdown("---")
st.markdown("""
    <div style="text-align: center; color: #666; font-size: 0.8rem; padding: 0.75rem 0;">
        © 2023 Insightify | Powered by AI | v2.1
    </div>
""", unsafe_allow_html=True)