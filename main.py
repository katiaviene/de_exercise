import pandas as pd
import sqlite3



def read_file(file):
    df = pd.read_csv(file)
    return df

def write_data(df, connection, table):
    df.to_sql(table, connection, if_exists="replace", index=False)

def init_db():
    conn = sqlite3.connect(':memory:')
    return conn

def read_query(query, connection):
    df = pd.read_sql(query, connection)
    return df
    
if __name__ == "__main__":
    conn = init_db()
    df = read_file("data\priceplan_hierarchy_anonymized.csv")
    write_data(df, conn, "priceplan")
    df = read_file("data\subscriptions_history_sample_anonymized.csv")
    write_data(df, conn, "subscriptions")
    
    # 1. How many price plans do we have?
    query = "SELECT COUNT(DISTINCT soc_pp_code) FROM priceplan"
    print(read_query(query, conn))
    
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
    print(read_query(query, conn))
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
    print(read_query(query, conn))
    
    # 4. How many times did customers switch from a less expensive to a more expensive subscription?
    # Swich - the same date, 
    query = """
            SELECT 
            count(*)
            
            FROM (
            SELECT
            *, 
            LEAD(rate) over (partition by subscriber_id order by effective_date) as next_rate,
            LEAD(effective_date) over(partition by subscriber_id order by effective_date) as next_eff_date
            FROM subscriptions) sub
            where sub.next_rate > sub.rate and julianday(sub.next_eff_date) -  julianday(sub.expiration_date) <=1
            and sub.rate != 0
    """
    print(read_query(query, conn))
    
    
    # 5. Which week of which year did the majority of subscriptions expire?
    query = """
            SELECT
            strftime('%Y', expiration_date) as year,
            strftime('%W', expiration_date) + 1 as week,
            count(subscriber_id) as cnt
            from subscriptions
            group by 
            strftime('%Y', expiration_date),
            strftime('%W', expiration_date)
            Order by count(subscriber_id) desc
            LIMIT 1
    """
    print(read_query(query, conn))
    
    # 6.How many new customers have been added on 2018-12-12? How many existing customers renewed their subscriptions on 2018-12-12?
    query = """
        SELECT 
        CASE
         WHEN before_date is null then 'new_customer'
         ELSE 'existing_customer'
        END,
        count(subscriber_id) as count
        FROM (
         SELECT *,
         lag(effective_date) over (partition by subscriber_id order by effective_date) as before_date
         FROM subscriptions ) sub
         WHERE date(effective_date) = '2018-12-12'
        GROUP BY 
            CASE
         WHEN before_date is null then 'new_customer'
         ELSE 'existing_customer'
        END
        
    """
    print(read_query(query, conn))
    
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
            left join 
            subscriptions sub
            on strftime('%Y', sub.effective_date) = rate.year
            and strftime('%W', sub.effective_date)+1 = rate.week
            and sub.rate = rate.max_rate
            left join priceplan pp
            on pp.soc_pp_code = sub.soc_pp_code
    """
    
    print(read_query(query, conn))
    
    