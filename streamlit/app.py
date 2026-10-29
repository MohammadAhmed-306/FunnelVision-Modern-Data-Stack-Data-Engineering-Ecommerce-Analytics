import os

import pandas as pd
import plotly.express as px
import streamlit as st
import snowflake.connector


# ==================================================
# 1. PAGE CONFIGURATION
# ==================================================
st.set_page_config(
    page_title="FunnelVision",
    page_icon="📊",
    layout="wide",
)

DATABASE = os.getenv("SNOWFLAKE_DATABASE", "FUNNELVISION_DB")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "DBT_DEV")

st.title("FunnelVision")
st.caption("E-Commerce Marketing Funnel Analytics")


# ==================================================
# 2. SNOWFLAKE CONNECTION
# ==================================================
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=DATABASE,
        schema=SCHEMA,
        role=os.environ.get("SNOWFLAKE_ROLE") or None,
    )


@st.cache_data(ttl=300)
def query_df(sql, params=None):
    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(sql, params or {})
        columns = [column[0] for column in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        cursor.close()


def table_name(name):
    return f'"{DATABASE}"."{SCHEMA}"."{name}"'


# Consistent channel cleanup for all SQL queries.
def channel_expression(alias=""):
    prefix = f"{alias}." if alias else ""
    source = f"{prefix}UTM_SOURCE"

    return f"""
        CASE
            WHEN {source} IS NULL
                OR TRIM({source}) = ''
                OR UPPER(TRIM({source})) = 'NULL'
            THEN 'Unknown'
            ELSE TRIM({source})
        END
    """


# ==================================================
# 3. DASHBOARD
# ==================================================
try:
    sessions = table_name("FCT_SESSIONS")
    orders = table_name("FCT_ORDERS")
    products = table_name("DIM_PRODUCT")
    item_refunds = table_name("INT_ORDER_ITEMS_WITH_REFUNDS")

    channel_expr = channel_expression("s")
    raw_channel_expr = channel_expression()

    # --------------------------------------------------
    # Available date range
    # --------------------------------------------------
    date_data = query_df(
        f"""
        SELECT
            MIN(SESSION_CREATED_AT)::DATE AS MIN_DATE,
            MAX(SESSION_CREATED_AT)::DATE AS MAX_DATE
        FROM {sessions}
        """
    )

    if (
        date_data.empty
        or pd.isna(date_data.iloc[0]["MIN_DATE"])
        or pd.isna(date_data.iloc[0]["MAX_DATE"])
    ):
        st.warning("No session data was found in FCT_SESSIONS.")
        st.stop()

    min_date = pd.Timestamp(date_data.iloc[0]["MIN_DATE"]).date()
    max_date = pd.Timestamp(date_data.iloc[0]["MAX_DATE"]).date()

    # --------------------------------------------------
    # Marketing channel options
    # --------------------------------------------------
    channel_data = query_df(
        f"""
        SELECT DISTINCT
            {raw_channel_expr} AS CHANNEL
        FROM {sessions}
        ORDER BY CHANNEL
        """
    )

    channel_options = channel_data["CHANNEL"].tolist()

    # --------------------------------------------------
    # Dashboard filters
    # --------------------------------------------------
    st.sidebar.header("Dashboard Filters")

    start_date = st.sidebar.date_input(
        "Start date",
        value=min_date,
        min_value=min_date,
        max_value=max_date,
    )

    end_date = st.sidebar.date_input(
        "End date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
    )

    selected_channels = st.sidebar.multiselect(
        "Marketing channel",
        options=channel_options,
        default=channel_options,
    )

    if start_date > end_date:
        st.error("Start date must be on or before end date.")
        st.stop()

    if not selected_channels:
        st.info("Select at least one marketing channel.")
        st.stop()

    # Quote selected values safely for the SQL IN clauses.
    quoted_channels = ", ".join(
        "'" + str(channel).replace("'", "''") + "'"
        for channel in selected_channels
    )

    date_params = {
        "start_date": start_date,
        "end_date": end_date,
    }

    # ==================================================
    # 4. KPI CARDS
    # ==================================================
    session_kpis = query_df(
        f"""
        SELECT
            COUNT(*) AS SESSIONS,
            COALESCE(SUM(CONVERTED_TO_ORDER), 0)
                AS CONVERTED_SESSIONS,
            COALESCE(SUM(SESSION_REVENUE), 0)
                AS SESSION_REVENUE
        FROM {sessions}
        WHERE SESSION_CREATED_AT::DATE
              BETWEEN %(start_date)s AND %(end_date)s
          AND {raw_channel_expr}
              IN ({quoted_channels})
        """,
        date_params,
    )

    order_kpis = query_df(
        f"""
        SELECT
            COUNT(DISTINCT o.ORDER_ID) AS ORDERS,
            COALESCE(SUM(o.REVENUE), 0) AS ORDER_REVENUE
        FROM {orders} o
        INNER JOIN {sessions} s
            ON o.WEBSITE_SESSION_ID = s.WEBSITE_SESSION_ID
        WHERE o.ORDER_DATE
              BETWEEN %(start_date)s AND %(end_date)s
          AND s.SESSION_CREATED_AT::DATE
              BETWEEN %(start_date)s AND %(end_date)s
          AND {channel_expr}
              IN ({quoted_channels})
        """,
        date_params,
    )

    session_count = int(session_kpis.iloc[0]["SESSIONS"])
    converted_count = int(
        session_kpis.iloc[0]["CONVERTED_SESSIONS"]
    )
    order_count = int(order_kpis.iloc[0]["ORDERS"])
    revenue = float(order_kpis.iloc[0]["ORDER_REVENUE"])

    conversion_rate = (
        converted_count / session_count * 100
        if session_count > 0
        else 0
    )

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Website Sessions", f"{session_count:,}")
    col2.metric("Orders", f"{order_count:,}")
    col3.metric("Order Revenue", f"${revenue:,.2f}")
    col4.metric("Session Conversion", f"{conversion_rate:.2f}%")

    st.divider()

    # ==================================================
    # 5. REVENUE BY MARKETING CHANNEL
    # ==================================================
    st.subheader("Revenue by Marketing Channel")

    revenue_channel = query_df(
        f"""
        SELECT
            {channel_expr} AS CHANNEL,
            SUM(o.REVENUE) AS REVENUE
        FROM {sessions} s
        INNER JOIN {orders} o
            ON s.WEBSITE_SESSION_ID = o.WEBSITE_SESSION_ID
        WHERE s.SESSION_CREATED_AT::DATE
              BETWEEN %(start_date)s AND %(end_date)s
          AND o.ORDER_DATE
              BETWEEN %(start_date)s AND %(end_date)s
          AND {channel_expr}
              IN ({quoted_channels})
        GROUP BY 1
        ORDER BY REVENUE DESC
        """,
        date_params,
    )

    if not revenue_channel.empty:
        fig_channel = px.bar(
            revenue_channel,
            x="CHANNEL",
            y="REVENUE",
            title="Order Revenue by Channel",
            labels={
                "CHANNEL": "Marketing Channel",
                "REVENUE": "Revenue ($)",
            },
            text_auto=".2s",
        )
        st.plotly_chart(fig_channel, use_container_width=True)
    else:
        st.info("No channel revenue found for these filters.")

    left, right = st.columns(2)

    # ==================================================
    # 6. MONTHLY ORDERS
    # ==================================================
    with left:
        st.subheader("Monthly Orders")

        monthly = query_df(
            f"""
            SELECT
                DATE_TRUNC('MONTH', o.ORDER_DATE)::DATE AS MONTH,
                COUNT(DISTINCT o.ORDER_ID) AS ORDERS,
                SUM(o.REVENUE) AS REVENUE
            FROM {orders} o
            INNER JOIN {sessions} s
                ON o.WEBSITE_SESSION_ID = s.WEBSITE_SESSION_ID
            WHERE o.ORDER_DATE
                  BETWEEN %(start_date)s AND %(end_date)s
              AND s.SESSION_CREATED_AT::DATE
                  BETWEEN %(start_date)s AND %(end_date)s
              AND {channel_expr}
                  IN ({quoted_channels})
            GROUP BY 1
            ORDER BY 1
            """,
            date_params,
        )

        if not monthly.empty:
            fig_monthly = px.line(
                monthly,
                x="MONTH",
                y="ORDERS",
                markers=True,
                title="Orders Over Time",
                labels={
                    "MONTH": "Month",
                    "ORDERS": "Number of Orders",
                },
            )
            st.plotly_chart(fig_monthly, use_container_width=True)
        else:
            st.info("No orders found for this date range.")

    # ==================================================
    # 7. REFUND RATE BY PRODUCT
    # ==================================================
    with right:
        st.subheader("Refund Rate by Product")

        refund_data = query_df(
            f"""
            SELECT
                COALESCE(
                    p.PRODUCT_NAME,
                    'Unknown product'
                ) AS PRODUCT,
                SUM(i.TOTAL_REFUNDED) AS REFUNDED_AMOUNT,
                SUM(i.PRICE_USD) AS ORIGINAL_REVENUE,
                ROUND(
                    100.0 * SUM(i.TOTAL_REFUNDED)
                    / NULLIF(SUM(i.PRICE_USD), 0),
                    2
                ) AS REFUND_RATE
            FROM {item_refunds} i
            LEFT JOIN {products} p
                ON i.PRODUCT_ID = p.PRODUCT_ID
            GROUP BY 1
            HAVING SUM(i.PRICE_USD) > 0
            ORDER BY REFUND_RATE DESC
            LIMIT 10
            """
        )

        if not refund_data.empty:
            fig_refunds = px.bar(
                refund_data,
                x="REFUND_RATE",
                y="PRODUCT",
                orientation="h",
                title="Top 10 Products by Refund Rate",
                labels={
                    "REFUND_RATE": "Refund Rate (%)",
                    "PRODUCT": "Product",
                },
                text="REFUND_RATE",
                hover_data=[
                    "REFUNDED_AMOUNT",
                    "ORIGINAL_REVENUE",
                ],
            )

            fig_refunds.update_traces(
                texttemplate="%{text:.2f}%",
                textposition="outside",
                cliponaxis=False,
            )

            fig_refunds.update_layout(
                xaxis_title="Refund Rate (%)",
                yaxis_title="Product",
                yaxis={"categoryorder": "total ascending"},
            )

            st.plotly_chart(
                fig_refunds,
                use_container_width=True,
            )
        else:
            st.info("No refundable item revenue was found.")

    # ==================================================
    # 8. REVENUE BY DEVICE
    # ==================================================
    st.subheader("Revenue by Device")

    device_revenue = query_df(
        f"""
        SELECT
            COALESCE(s.DEVICE_TYPE, 'Unknown') AS DEVICE,
            SUM(o.REVENUE) AS REVENUE
        FROM {sessions} s
        INNER JOIN {orders} o
            ON s.WEBSITE_SESSION_ID = o.WEBSITE_SESSION_ID
        WHERE s.SESSION_CREATED_AT::DATE
              BETWEEN %(start_date)s AND %(end_date)s
          AND o.ORDER_DATE
              BETWEEN %(start_date)s AND %(end_date)s
          AND {channel_expr}
              IN ({quoted_channels})
        GROUP BY 1
        ORDER BY REVENUE DESC
        """,
        date_params,
    )

    if not device_revenue.empty:
        fig_device = px.pie(
            device_revenue,
            names="DEVICE",
            values="REVENUE",
            title="Revenue Share by Device",
        )
        st.plotly_chart(fig_device, use_container_width=True)
    else:
        st.info("No device revenue found for these filters.")

    # ==================================================
    # 9. FOOTER
    # ==================================================
    st.caption(
        "Data source: Snowflake dbt models. "
        "Query results are cached for five minutes."
    )

except Exception as error:
    st.error("The dashboard could not load data from Snowflake.")
    st.exception(error)



