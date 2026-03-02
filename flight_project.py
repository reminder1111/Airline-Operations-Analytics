# ============================================================
#         FLIGHT DATA ANALYTICS PROJECT
#   SQL + PyMySQL + SQLAlchemy + Visualization
# ============================================================

# -----------------------------------------------
# STEP 1: IMPORTS
# -----------------------------------------------
import pymysql
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("   ✈️  FLIGHT DATA ANALYTICS PROJECT STARTING...")
print("=" * 60)

# -----------------------------------------------
# STEP 2: DATABASE CONNECTION
# -----------------------------------------------
password = quote_plus('Mumm@1994')       # <-- apna password
DATABASE  = 'neha'                        # <-- apna database name

DATABASE_URL = f'mysql+pymysql://root:{password}@localhost/{DATABASE}'
engine = create_engine(DATABASE_URL, echo=False)
print("\n✅ Database Connected Successfully!")

# -----------------------------------------------
# STEP 3: LOAD CSV & INSERT INTO MYSQL TABLE
# -----------------------------------------------
df = pd.read_csv('flights.csv')
df.to_sql('flights', con=engine, if_exists='replace', index=False)
print(f"✅ {len(df)} rows inserted into 'flights' table in MySQL!")

# -----------------------------------------------
# STEP 4: SQL QUERIES
# -----------------------------------------------
conn = engine.connect()

print("\n" + "=" * 60)
print("         📊 SQL ANALYSIS RESULTS")
print("=" * 60)

# Q1 - Total Flights per Airline
q1 = pd.read_sql(text("""
    SELECT airline, COUNT(*) AS total_flights
    FROM flights
    GROUP BY airline
    ORDER BY total_flights DESC
"""), conn)
print("\n🔹 Q1: Total Flights per Airline")
print(q1.to_string(index=False))

# Q2 - Average Occupancy Rate per Airline
q2 = pd.read_sql(text("""
    SELECT airline, ROUND(AVG(occupancy_rate), 2) AS avg_occupancy
    FROM flights
    GROUP BY airline
    ORDER BY avg_occupancy DESC
"""), conn)
print("\n🔹 Q2: Average Occupancy Rate per Airline (%)")
print(q2.to_string(index=False))

# Q3 - Top 5 Busiest Routes
q3 = pd.read_sql(text("""
    SELECT source, destination, COUNT(*) AS total_flights
    FROM flights
    GROUP BY source, destination
    ORDER BY total_flights DESC
    LIMIT 5
"""), conn)
print("\n🔹 Q3: Top 5 Busiest Routes")
print(q3.to_string(index=False))

# Q4 - Monthly Flight Count + Avg Occupancy
q4 = pd.read_sql(text("""
    SELECT month,
           COUNT(*) AS total_flights,
           ROUND(AVG(occupancy_rate), 2) AS avg_occupancy
    FROM flights
    GROUP BY month
    ORDER BY total_flights DESC
"""), conn)
print("\n🔹 Q4: Monthly Flight Count & Avg Occupancy")
print(q4.to_string(index=False))

# Q5 - Revenue per Airline
q5 = pd.read_sql(text("""
    SELECT airline, SUM(revenue) AS total_revenue
    FROM flights
    GROUP BY airline
    ORDER BY total_revenue DESC
"""), conn)
print("\n🔹 Q5: Total Revenue per Airline")
print(q5.to_string(index=False))

# Q6 - Avg Delay per Airline
q6 = pd.read_sql(text("""
    SELECT airline, ROUND(AVG(delay_minutes), 2) AS avg_delay
    FROM flights
    GROUP BY airline
    ORDER BY avg_delay DESC
"""), conn)
print("\n🔹 Q6: Average Delay per Airline (minutes)")
print(q6.to_string(index=False))

# Q7 - Cancellation Rate per Airline
q7 = pd.read_sql(text("""
    SELECT airline,
           COUNT(*) AS total_flights,
           SUM(is_cancelled) AS cancelled,
           ROUND(SUM(is_cancelled) * 100.0 / COUNT(*), 2) AS cancellation_rate
    FROM flights
    GROUP BY airline
    ORDER BY cancellation_rate DESC
"""), conn)
print("\n🔹 Q7: Cancellation Rate per Airline (%)")
print(q7.to_string(index=False))

# Q8 - Window Function: Rank airlines by revenue per route
q8 = pd.read_sql(text("""
    SELECT airline, source, destination,
           SUM(revenue) AS route_revenue,
           RANK() OVER (PARTITION BY source ORDER BY SUM(revenue) DESC) AS revenue_rank
    FROM flights
    GROUP BY airline, source, destination
    ORDER BY source, revenue_rank
    LIMIT 15
"""), conn)
print("\n🔹 Q8: Revenue Rank per Route (Window Function)")
print(q8.to_string(index=False))

# Q9 - CTE: High Occupancy Flights (above average)
q9 = pd.read_sql(text("""
    WITH avg_occ AS (
        SELECT AVG(occupancy_rate) AS overall_avg FROM flights
    )
    SELECT airline, source, destination, occupancy_rate
    FROM flights, avg_occ
    WHERE occupancy_rate > overall_avg
    ORDER BY occupancy_rate DESC
    LIMIT 10
"""), conn)
print("\n🔹 Q9: CTE - Top Flights Above Avg Occupancy")
print(q9.to_string(index=False))

# Q10 - Underperforming Routes (low occupancy + low revenue)
q10 = pd.read_sql(text("""
    SELECT source, destination,
           ROUND(AVG(occupancy_rate), 2) AS avg_occupancy,
           SUM(revenue) AS total_revenue
    FROM flights
    GROUP BY source, destination
    HAVING avg_occupancy < 55
    ORDER BY avg_occupancy ASC
    LIMIT 10
"""), conn)
print("\n🔹 Q10: Underperforming Routes (Occupancy < 55%)")
print(q10.to_string(index=False))

conn.close()

# -----------------------------------------------
# STEP 5: DATA VISUALIZATION (9 Charts)
# -----------------------------------------------
print("\n📊 Generating Visualizations... Please wait...")

conn = engine.connect()
df_full = pd.read_sql(text("SELECT * FROM flights"), conn)
conn.close()

# Month order
month_order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
colors5 = ['#4C72B0','#DD8452','#55A868','#C44E52','#8172B2']

fig, axes = plt.subplots(3, 3, figsize=(22, 18))
fig.suptitle('✈️  Flight Data Analytics Dashboard', fontsize=24, fontweight='bold')
plt.subplots_adjust(hspace=0.55, wspace=0.4)

# --- Chart 1: Total Flights per Airline ---
ax1 = axes[0][0]
flight_counts = df_full['airline'].value_counts()
bars = ax1.bar(flight_counts.index, flight_counts.values, color=colors5, edgecolor='white')
ax1.set_title('Total Flights per Airline', fontweight='bold')
ax1.set_xlabel('Airline')
ax1.set_ylabel('Number of Flights')
ax1.tick_params(axis='x', rotation=20)
for bar in bars:
    ax1.text(bar.get_x()+bar.get_width()/2, bar.get_height()+1,
             str(int(bar.get_height())), ha='center', va='bottom', fontsize=9)

# --- Chart 2: Avg Occupancy Rate per Airline ---
ax2 = axes[0][1]
occ = df_full.groupby('airline')['occupancy_rate'].mean().sort_values(ascending=False)
bars2 = ax2.bar(occ.index, occ.values, color=colors5, edgecolor='white')
ax2.set_title('Avg Occupancy Rate per Airline (%)', fontweight='bold')
ax2.set_xlabel('Airline')
ax2.set_ylabel('Occupancy %')
ax2.tick_params(axis='x', rotation=20)
ax2.axhline(occ.mean(), color='red', linestyle='--', label=f'Avg: {occ.mean():.1f}%')
ax2.legend(fontsize=8)
for bar in bars2:
    ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.3,
             f'{bar.get_height():.1f}%', ha='center', va='bottom', fontsize=9)

# --- Chart 3: Revenue per Airline (Pie Chart) ---
ax3 = axes[0][2]
rev = df_full.groupby('airline')['revenue'].sum()
ax3.pie(rev.values, labels=rev.index, autopct='%1.1f%%', colors=colors5,
        startangle=140, textprops={'fontsize': 9})
ax3.set_title('Revenue Share per Airline', fontweight='bold')

# --- Chart 4: Monthly Flight Trend ---
ax4 = axes[1][0]
monthly = df_full.groupby('month').size().reindex(month_order, fill_value=0)
ax4.plot(monthly.index, monthly.values, marker='o', color='#4C72B0', linewidth=2.5, markersize=7)
ax4.fill_between(monthly.index, monthly.values, alpha=0.15, color='#4C72B0')
ax4.set_title('Monthly Flight Count (Seasonal Trend)', fontweight='bold')
ax4.set_xlabel('Month')
ax4.set_ylabel('Flights')
ax4.tick_params(axis='x', rotation=30)
ax4.grid(axis='y', linestyle='--', alpha=0.5)

# --- Chart 5: Monthly Avg Occupancy ---
ax5 = axes[1][1]
monthly_occ = df_full.groupby('month')['occupancy_rate'].mean().reindex(month_order, fill_value=0)
ax5.bar(monthly_occ.index, monthly_occ.values, color='#55A868', edgecolor='white')
ax5.set_title('Monthly Avg Occupancy Rate (%)', fontweight='bold')
ax5.set_xlabel('Month')
ax5.set_ylabel('Occupancy %')
ax5.tick_params(axis='x', rotation=30)
ax5.axhline(monthly_occ.mean(), color='red', linestyle='--', label=f'Avg: {monthly_occ.mean():.1f}%')
ax5.legend(fontsize=8)

# --- Chart 6: Top 10 Busiest Routes ---
ax6 = axes[1][2]
df_full['route'] = df_full['source'] + ' → ' + df_full['destination']
top_routes = df_full['route'].value_counts().head(10)
ax6.barh(top_routes.index, top_routes.values, color='#C44E52', edgecolor='white')
ax6.set_title('Top 10 Busiest Routes', fontweight='bold')
ax6.set_xlabel('Number of Flights')
ax6.invert_yaxis()

# --- Chart 7: Delay Distribution ---
ax7 = axes[2][0]
for i, airline in enumerate(df_full['airline'].unique()):
    subset = df_full[df_full['airline'] == airline]['delay_minutes']
    ax7.hist(subset, bins=15, alpha=0.6, label=airline, color=colors5[i])
ax7.set_title('Delay Distribution per Airline', fontweight='bold')
ax7.set_xlabel('Delay (minutes)')
ax7.set_ylabel('Frequency')
ax7.legend(fontsize=7)

# --- Chart 8: Cancellation Rate per Airline ---
ax8 = axes[2][1]
cancel = df_full.groupby('airline')['is_cancelled'].mean() * 100
bars8 = ax8.bar(cancel.index, cancel.values, color=colors5, edgecolor='white')
ax8.set_title('Cancellation Rate per Airline (%)', fontweight='bold')
ax8.set_xlabel('Airline')
ax8.set_ylabel('Cancellation %')
ax8.tick_params(axis='x', rotation=20)
for bar in bars8:
    ax8.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.05,
             f'{bar.get_height():.1f}%', ha='center', va='bottom', fontsize=9)

# --- Chart 9: Ticket Price vs Occupancy (Scatter) ---
ax9 = axes[2][2]
for i, airline in enumerate(df_full['airline'].unique()):
    subset = df_full[df_full['airline'] == airline]
    ax9.scatter(subset['ticket_price'], subset['occupancy_rate'],
                alpha=0.5, label=airline, color=colors5[i], s=30)
ax9.set_title('Ticket Price vs Occupancy Rate', fontweight='bold')
ax9.set_xlabel('Ticket Price (₹)')
ax9.set_ylabel('Occupancy %')
ax9.legend(fontsize=7)

plt.savefig('flight_dashboard.png', dpi=150, bbox_inches='tight')
plt.show()
print("\n✅ Dashboard saved as 'flight_dashboard.png'")

print("\n" + "=" * 60)
print("   🎉 PROJECT COMPLETE! ALL STEPS EXECUTED SUCCESSFULLY!")
print("=" * 60)
print("""
SUMMARY:
✅ MySQL Database Connected
✅ 459 Flight Records Inserted
✅ 10 SQL Queries Executed (CTEs, Window Functions, Aggregations)
✅ 9 Visualizations Generated
✅ Dashboard Saved

POWER BI STEPS (next):
1. Open Power BI Desktop
2. Get Data → MySQL Database
3. Server: localhost | Database: neha
4. Select 'flights' table → Load
5. Create visuals using same KPIs
""")
