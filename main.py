import pandas as pd
import sqlite3
from tabulate import tabulate


def init_db():
    conn = sqlite3.connect(':memory:')
    return conn


def read_file(file):
    df = pd.read_csv(file)
    return df


def write_data(df, connection, table):
    df.to_sql(table, connection, if_exists="replace", index=False)


def read_query(query, connection):
    df = pd.read_sql(query, connection)
    return tabulate(df, headers="keys", tablefmt='pipe')


def present(title, query, conn):
    print(title)
    print(read_query(query, conn))
    input("")
    
    
if __name__ == "__main__":
    
    conn = init_db()
    df = read_file("data\priceplan_hierarchy_anonymized.csv")
    write_data(df, conn, "priceplan")
    df = read_file("data\subscriptions_history_sample_anonymized.csv")
    write_data(df, conn, "subscriptions")
    
    # 1. How many price plans do we have?
    query = "SELECT COUNT(DISTINCT soc_pp_code) as plan_count FROM priceplan"
    present("Priceplans:", query, conn)

    
    
    # 2. What segment does the most expensive subscription belong to?
    query = """
                SELECT
                    pp.product_segment,
                    pp.soc_pp_code,
                    max_rate_plan.max_rate
                FROM priceplan pp
                JOIN
                    (SELECT 
                        soc_pp_code,
                        rate as max_rate
                    FROM subscriptions 
                    ORDER BY rate DESC
                    LIMIT 1) max_rate_plan
                ON pp.soc_pp_code = max_rate_plan.soc_pp_code
                
                """
    present("Most expensive plan", query, conn)

    # 3. How much does the most popular subscription cost?
    query = """
    
            SELECT
                DISTINCT
                sub.soc_pp_code,
                sub.rate,
                plan_cnt.plan_count
            FROM subscriptions sub
            JOIN 
                (SELECT 
                    soc_pp_code,
                    count(soc_pp_code) plan_count
                FROM subscriptions
                GROUP BY soc_pp_code
                ORDER BY plan_count DESC
                LIMIT 1) plan_cnt
            ON plan_cnt.soc_pp_code = sub.soc_pp_code
            
    """
    present("Most popular plan", query, conn)

    
    # 4. How many times did customers switch from a less expensive to a more expensive subscription?
    # Assumpition: not counting prepaid-postpaid transition, can't be considered less expensive - more expensive subscription switch
    query = """
            SELECT 
            COUNT(*) as customer_count
            FROM (
                SELECT
                *, 
                LEAD(rate) OVER (PARTITION BY  subscriber_id ORDER BY effective_date) as next_rate,
                LEAD(effective_date) OVER (PARTITION BY subscriber_id ORDER BY effective_date) as next_eff_date
            FROM subscriptions) sub
            left join priceplan pp
            on sub.soc_pp_code = pp.soc_pp_code
            WHERE 1=1
            AND sub.next_rate > sub.rate 
            AND pp.product_payment_type != 'Prepaid'
    """
    present("Subscription switch", query, conn)

    
    
    # 5. Which week of which year did the majority of subscriptions expire?
    query = """
            SELECT
                strftime('%Y', expiration_date) as year,
                strftime('%W', expiration_date) + 1 as week,
                count(subscriber_id) as cnt
            FROM subscriptions
            GROUP BY
            strftime('%Y', expiration_date),
            strftime('%W', expiration_date)
            ORDER BY count(subscriber_id) desc
            LIMIT 1
            
    """
    present("Majority expired", query, conn)

    # 6.How many new customers have been added on 2018-12-12? How many existing customers renewed their subscriptions on 2018-12-12?
    query = """
        SELECT 
            CASE
                WHEN before_date IS NULL THEN 'new_customer'
                ELSE 'existing_customer'
            END as customer,
            count(subscriber_id) as count
        FROM (
            SELECT 
                *,
                LAG(effective_date) OVER (PARTITION BY subscriber_id ORDER BY effective_date) as before_date
            FROM subscriptions ) sub
            
        WHERE date(effective_date) = '2018-12-12'
        GROUP BY 
            CASE
                 WHEN before_date IS NULL THEN 'new_customer'
                 ELSE 'existing_customer'
            END
        
    """
    
    present("Renewed on 2018-12-12", query, conn)

    
    # 7. Every week of every year lists the most expensive subscription, its number, segment, and rate.
    query = """
            SELECT 
            DISTINCT
            rate.year,
            rate.week,
            rate.max_rate,
            sub.soc_pp_code,
            pp.product_segment
            
            FROM 
            (
                SELECT
                    strftime('%Y', effective_date) as year,
                    strftime('%W', effective_date) + 1 as week,
                    max(rate) as max_rate
                FROM subscriptions
                GROUP BY 
                strftime('%Y', effective_date),
                strftime('%W', effective_date)) rate
                
            LEFT JOIN subscriptions sub
            ON strftime('%Y', sub.effective_date) = rate.year
            AND strftime('%W', sub.effective_date)+1 = rate.week
            AND sub.rate = rate.max_rate
            
            LEFT JOIN  priceplan pp
            ON pp.soc_pp_code = sub.soc_pp_code
            
    """
    present("Weekly metrics", query, conn)

    


    