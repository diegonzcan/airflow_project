@SQL
CREATE VIEW exchange_rates.DATA as (
WITH CTE AS (
SELECT 
fr_curr_cd,
fr_curr_nm,
to_curr_cd,
to_curr_nm,
exchange_rt,
EXTRACT(DATE FROM last_refreshed_dt) AS dt,
EXTRACT(TIME FROM last_refreshed_dt) AS time,
tz,
bid_pr,
ask_pr
FROM `pipeline-387723.exchange_rates.forex`),
DATA AS (
SELECT *,
LAG(exchange_rt,1) OVER (PARTITION BY fr_curr_cd ORDER BY dt ASC) last_rate,
LAG(ask_pr,1) OVER (PARTITION BY fr_curr_cd ORDER BY dt ASC) last_ask_pr,
LAG(bid_pr,1) OVER (PARTITION BY fr_curr_cd ORDER BY dt ASC) last_bid_pr
FROM cte ) 
SELECT *,
  ((exchange_rt - last_rate)/last_rate)*100 as pct_rate_change,
  ((ask_pr - last_ask_pr)/last_ask_pr)*100 as pct_ask_pr_change,
  ((bid_pr - last_bid_pr)/last_bid_pr)*100 as pct_bid_pr_change
FROM DATA
);
