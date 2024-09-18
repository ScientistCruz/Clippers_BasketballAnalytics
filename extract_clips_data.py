import pyodbc
import sqlalchemy
import pandas as pd
import os
import pathlib
import sys
import pandas as pd

dname = os.path.dirname(os.path.abspath(__file__))
utils = os.path.join(dname,'utilities')
dev_test_data = os.path.join(dname,'dev_test_data')

sys.path.insert(0, utils)

import utilities
from utilities.utils import sql_server


if __name__ == '__main__':
    connection = sql_server(server = 'sql_server' , username = 'sa', port = 1443, password = 'tEST1234')
    print(connection.connection)

    connection.create_database(database='lac_fullstack_dev')
    connection.create_database(database='lac_fullstack_dev_staging')

    # datetimes
    onlyfiles = [f for f in os.listdir(dev_test_data) if os.path.isfile(os.path.join(dev_test_data, f))]
    print(onlyfiles)

    for file in os.listdir(dev_test_data):

        if '.json' in file:
            file_df = pd.read_json(os.path.join(dev_test_data, file))
            db_name = file.replace('.json', '')
        else:
            pass

        if db_name == 'player':
            merge_column = ['player_id']
            file_df[merge_column[0]] = file_df[[merge_column[0]]].astype('int64')
        elif db_name == 'roster':
            merge_column = ['team_id', 'player_id']
            file_df[merge_column[0]] = file_df[[merge_column[0]]].astype('int64')
            file_df[merge_column[1]] = file_df[[merge_column[1]]].astype('int64')

        elif db_name == 'game_schedule':
            merge_column = ['game_id']
            file_df[merge_column[0]] = file_df[[merge_column[0]]].astype('int64')
            file_df['home_score'] = file_df['home_score'].astype('int64')
            file_df['home_id'] = file_df['home_id'].astype('int64')
            file_df['away_score'] = file_df['away_score'].astype('int64')
            file_df['away_id'] = file_df['away_id'].astype('int64')            

        elif db_name == 'team_affiliate':
            merge_column = ['nba_teamId']
            file_df[merge_column[0]] = file_df[[merge_column[0]]].astype('int64')

            file_df['glg_teamId'] = file_df['glg_teamId'].fillna(0)

            file_df['glg_teamId'] = file_df['glg_teamId'].astype('string')
            file_df['glg_teamId'] = file_df['glg_teamId'].str.replace('.0', '')

            file_df['glg_teamId'] = file_df['glg_teamId'].astype('int64')

        elif db_name == 'lineup':
            # merge_column = ['team_id', 'player_id', 'lineup_num']
            merge_column = ['team_id', 'lineup_num', 'game_id', 'player_id']
            file_df = file_df.astype('int64')
        elif db_name == 'team':
            merge_column = ['teamId']
            file_df[merge_column[0]] = file_df[[merge_column[0]]].astype('int64')

        else:
            print('Table [merge column] needs to mapped.')


        connection.sql_query_bt(file_df,db_name, merge_column, 0)        

    # Question #2
    query_string_q2 = """Use lac_fullstack_dev
Go

CREATE VIEW question_02 AS

-- 2. Basic Queries (SQL)
-- a. Write a SQL query that can calculate team win-loss records, sorted by win percentage (defined as wins divided by games played)


WITH home_stats as (select a.*, b.teamName 
                    from (select sum(HOME_WIN) as W, sum(HOME_LOSE) as L,  home_id as team_id
                        from (select game_id, home_id, home_score, away_id, away_score,
                                case when home_score > away_score then 1
                                else 0
                                END 
                                as HOME_WIN,
                                case when home_score < away_score then 1
                                else 0
                                END 
                                as HOME_LOSE        


                                from lac_fullstack_dev.dbo.game_schedule a
                                -- where home_id = '1610612743'
                                )a 
                        group by home_id) a

                        left join lac_fullstack_dev.dbo.team b
                        on a.team_id = b.teamId),
        away_stats as (select a.*, b.teamName 
                        from (select sum(away_WIN) as W, sum(away_LOSE) as L,  away_id as team_id
                            from (select game_id, home_id, home_score, away_id, away_score,
                                    case when home_score < away_score then 1
                                    else 0
                                    END 
                                    as away_WIN,
                                    case when home_score > away_score then 1
                                    else 0
                                    END 
                                    as away_LOSE        


                                    from lac_fullstack_dev.dbo.game_schedule a
                                    -- where away_id = '1610612743'
                                    )a 
                            group by away_id) a

                        left join lac_fullstack_dev.dbo.team b
                        on a.team_id = b.teamId)

select * 
from (select sum(W) as wins, sum(L) as loses, cast(round(cast(sum(W) as decimal(16,2))/(sum(L) + sum(W)), 2) as decimal (16,2)) as win_percent, team_id, teamName 
    from (select * from home_stats
        union all
        select * from away_stats) a
    group by team_id, teamName) a
-- order by win_percent desc


-- i. Final table should include team name, games played, wins, losses, win percentage
-- b. In the same table, show how the team ranks (highest to lowest) in terms of games played, home games, and away games during this month of the season? Make sure your code can extend to additional months as data is added to the data set. For each, show both the number of games and the rank
"""
    try:
        connection.exceute_query(query_string_q2)
    except:
        error_value = str(sys.exc_info()[1])
        print(error_value)

    # Question #3

    query_string_q3 = """Use lac_fullstack_dev
Go

CREATE VIEW question_03 AS

-- WITH home_BBs_ro as (select home_id as team_id, game_date, ROW_NUMBER() OVER(ORDER BY home_id, game_date) as rn 
--                     from lac_fullstack_dev.dbo.game_schedule 
--                     -- where home_id = '1610612746'
--                     ),
--     away_BBs_ro as (select away_id as team_id, game_date, ROW_NUMBER() OVER(ORDER BY away_id, game_date) as rn 
--                     from lac_fullstack_dev.dbo.game_schedule 
--                     -- where away_id = '1610612746'
--                     ),
--     away_BBs as (select a.team_id, b.game_date as game_date_g1_bb, a.game_date as game_date_g2_bb from away_BBs_ro a
--                 left join away_BBs_ro b
--                 on a.rn = b.rn + 1 and a.team_id = b.team_id and DATEDIFF(day, b.game_date,a.game_date) = 1
--                 where b.team_id is not null
--                 ),
--     home_BBs as (select a.team_id, b.game_date as game_date_g1_bb, a.game_date as game_date_g2_bb from home_BBs_ro a
--                 left join home_BBs_ro b
--                 on a.rn = b.rn + 1 and a.team_id = b.team_id and DATEDIFF(day, b.game_date,a.game_date) = 1
--                 where b.team_id is not null
--                 )


-- -- select * 
-- -- from(select b.teamName, a.* , 'home' as bb_type from home_BBs a
-- --     left join lac_fullstack_dev.dbo.team b
-- --     on a.team_id = b.teamId


-- --     union all

-- --     select b.teamName, a.*, 'away' as bb_type from away_BBs a
-- --     left join lac_fullstack_dev.dbo.team b
-- --     on a.team_id = b.teamId) a
-- -- order by game_date_g1_bb asc



-- -- a. The NBA has a concept of a Back-to-Back (B2B) which is if a team played 2 days in a row (regardless of start time). 
-- -- For the data given which team had the most Home-Home B2Bs? 
-- select * , 'home' as bb_type from (select count(*) as total_home_BBs, teamName from (select b.teamName, a.* , 'home' as bb_type from home_BBs a
--         left join lac_fullstack_dev.dbo.team b
--         on a.team_id = b.teamId) a
--         group by teamName) a

-- where total_home_BBs = (select max(total_home_BBs) from (select count(*) as total_home_BBs, teamName from (select b.teamName, a.* , 'home' as bb_type from home_BBs a
--                         left join lac_fullstack_dev.dbo.team b
--                         on a.team_id = b.teamId) a
--                         group by teamName) a)

-- union all
-- -- Which had the most Away-Away B2Bs? 
-- select *, 'away' as bb_type from (select count(*) as total_home_BBs, teamName from (select b.teamName, a.* , 'away' as bb_type from away_BBs a
--         left join lac_fullstack_dev.dbo.team b
--         on a.team_id = b.teamId) a
--         group by teamName) a

-- where total_home_BBs = (select max(total_home_BBs) from (select count(*) as total_home_BBs, teamName from (select b.teamName, a.* , 'away' as bb_type from away_BBs a
--                         left join lac_fullstack_dev.dbo.team b
--                         on a.team_id = b.teamId) a
--                         group by teamName) a)


--------------------------------------
-- b. Which team(s) had the longest rest between 2 games and what were the days of the 2 games?

-- with games_ro as (select *, ROW_NUMBER() OVER(ORDER BY team_id, game_date) as rn from (select home_id as team_id, game_date from lac_fullstack_dev.dbo.game_schedule 
--                 UNION
--                 select away_id as team_id, game_date from lac_fullstack_dev.dbo.game_schedule ) a
--                 ),
--         games as (select a.team_id, b.game_date as game_date_g1, a.game_date as game_date_g2, datediff(day, cast(b.game_date as datetime), cast(a.game_date as datetime)) as rest_days from games_ro a
--                 left join games_ro b
--                 on a.rn = b.rn + 1 and a.team_id = b.team_id )

-- select b.teamName, a.* from games a
-- left join lac_fullstack_dev.dbo.team b
--         on a.team_id = b.teamId
-- where rest_days = (select max(rest_days) from games)


--------------------------------------

-- select * from lac_fullstack_dev.dbo.game_schedule 

with games_ro as (select *, ROW_NUMBER() OVER(PARTITION by team_id ORDER BY team_id, game_date) as rn from (select home_id as team_id, game_date from lac_fullstack_dev.dbo.game_schedule 
                UNION
                select away_id as team_id, game_date from lac_fullstack_dev.dbo.game_schedule ) a
                ), 
        games_ro_V2 as (select *, count(*) OVER (PARTITION by team_id ORDER BY team_id, game_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rn from (select home_id as team_id, game_date from lac_fullstack_dev.dbo.game_schedule 
                UNION
                select away_id as team_id, game_date from lac_fullstack_dev.dbo.game_schedule ) a
                )
--  select t.*,
--            datediff(day,
--                     lag(DATE) over (partition by NUMBER order by id),
--                     DATE) as diff 


-- -- select * from games_ro
-- select * from games_ro


-- select a.*,
--                    (case when lag(game_date) over (partition by team_id order by team_id, game_date) < dateadd(day, 1, game_date)
--                             then 1 else 0
--                     end) as grp_start
--                     from games_ro a

-- select a.team_id , a.game_date as og_gd, b.game_date from games_ro a
-- left join games_ro b
-- on a.team_id = b.team_id and abs(DATEDIFF(day,a.game_date,b.game_date)) <= 2
-- where a.team_id = '1610612747'




-- c. Additionally, write a query that ranks the teams based on the number of 3-in-4s (3 games over 4 days that is regardless of start time).
select count(*) as three_in_four, b.teamName 
from (select count(*) as total_games, team_id, og_gd
        from (select a.team_id , a.game_date as og_gd, b.game_date from games_ro a
        left join games_ro b
        on a.team_id = b.team_id and abs(DATEDIFF(day,a.game_date,b.game_date)) <= 2) a
        -- where team_id = '1610612747'
        group by team_id, og_gd
        ) a
left join lac_fullstack_dev.dbo.team b
on a.team_id = b.teamId        
group by b.teamName
order by count(*) desc"""
    try:
        connection.exceute_query(query_string_q3)
    except:
        error_value = str(sys.exc_info()[1])
        print(error_value)
    # Question #4

    query_string_q4 = """Use lac_fullstack_dev
Go

CREATE VIEW question_04 AS

-- select * from lac_fullstack_dev.dbo.game_schedule

-- 4.Lineups Queries (SQL): In answering any of these items, feel free creating intermediate temp tables, inline tables, or CTEs as needed.
-- a. Notice that in the lineup data each row corresponds to a given player, game, lineup_num, period. 
-- Write a SQL query that creates a “wide” table for the team (so a given row is now game_id, team_id, lineup_num, period, time_in, time_out, and the 5 players on the court)
-- i. Notice that time_in and time_out are in seconds, starting at 12 minutes (720) and going down to 0 minutes

-- select game_id, team_id, lineup_num,[1] as player_1, [2] as player_2, [3] as player_3, [4] as player_4, [5] as player_5 
-- from (select team_id, game_id, player_id, lineup_num, ROW_NUMBER() OVER (PARTITION BY game_id, team_id, lineup_num ORDER BY player_id) AS rn from lac_fullstack_dev.dbo.lineup
-- -- where team_id = '1610612747' and game_id = '66' and lineup_num = 5
-- ) a
-- PIVOT(max(player_id) 
-- for rn in ([1], [2], [3], [4], [5])) as pt

-- b. The field lineup_num changes as a player on either team gets substituted. 
-- Write a SQL query with the resultant table that stores when a player is continuously on the court for a given period (call this a stint)
-- select team_id, game_id, player_id, [period], time_in, time_out, ROW_NUMBER() OVER(PARTITION by game_id, team_id, [period] ORDER BY game_id, team_id, [period]) as rn from lac_fullstack_dev.dbo.lineup
-- where team_id = '1610612747' and game_id = '66' and player_id = '203076' and [period] = 1
-- order by period, lineup_num

with player_stint_ro as (select team_id, game_id, player_id, [period], lineup_num, time_in, time_out
-- , ROW_NUMBER() OVER(PARTITION by game_id, team_id, [period] ORDER BY game_id, team_id, [period], lineup_num) as rn
 from lac_fullstack_dev.dbo.lineup
                    -- where team_id = '1610612747' and game_id = '66' and player_id = '203076' and [period] = 1
                    ),
    player_stints as ( --home games
                    select a.game_id, b.game_date, c.teamName as team, d.teamName as opponent,
                    concat(e.first_name, ' ' , e.last_name) as player_name, 
                    -- period, period as stint_number, cast(RIGHT(CONVERT(CHAR(8),DATEADD(second,time_in,0),108),5) as nvarchar) as time_in, cast(RIGHT(CONVERT(CHAR(8),DATEADD(second,time_out,0),108),5) as nvarchar) as time_out
                    period, period as stint_number, RIGHT(CONVERT(CHAR(8),DATEADD(second,time_in,0),108),5) as time_in, RIGHT(CONVERT(CHAR(8),DATEADD(second,time_out,0),108),5) as time_out

                    from (select game_id, team_id, player_id, period,  max(time_in) as time_in, min(time_out) as time_out 
                        from (select a.* 
                            from player_stint_ro a
                            left join player_stint_ro b
                            on a.game_id = b.game_id and a.team_id =  b.team_id and a.player_id = b.player_id and a.[period] = b.[period] and a.lineup_num = b.lineup_num - 1
                            and a.time_out = b.time_in) a
                        group by game_id, team_id, player_id, period) a
                    -- group by game_id, team_id, player_id, period) a
                    left join lac_fullstack_dev.dbo.game_schedule b
                    on a.game_id = b.game_id and a.team_id = b.home_id
                    left join lac_fullstack_dev.dbo.team c
                    on a.team_id = c.teamId
                    left join lac_fullstack_dev.dbo.team d
                    on b.away_id = d.teamId
                    left join lac_fullstack_dev.dbo.player e
                    on a.player_id = e.player_id
                    where b.game_date is not null
                    
                    union all

                    --  away_games
                    select a.game_id, b.game_date, c.teamName as team, d.teamName as opponent,
                    concat(e.first_name, ' ' , e.last_name) as player_name, 
                    -- period, period as stint_number, CAST(RIGHT(CONVERT(CHAR(8),DATEADD(second,time_in,0),108),5) as nvarchar) as time_in, CAST(RIGHT(CONVERT(CHAR(8),DATEADD(second,time_out,0),108),5) as nvarchar) as time_out
                    period, period as stint_number, RIGHT(CONVERT(CHAR(8),DATEADD(second,time_in,0),108),5) as stint_start_time, RIGHT(CONVERT(CHAR(8),DATEADD(second,time_out,0),108),5) as stint_end_time

                    from (select game_id, team_id, player_id, period,  max(time_in) as time_in, min(time_out) as time_out 
                        from (select a.* 
                            from player_stint_ro a
                            left join player_stint_ro b
                            on a.game_id = b.game_id and a.team_id =  b.team_id and a.player_id = b.player_id and a.[period] = b.[period] and a.lineup_num = b.lineup_num - 1
                            and a.time_out = b.time_in) a
                        group by game_id, team_id, player_id, period) a
                    -- group by game_id, team_id, player_id, period) a
                    left join lac_fullstack_dev.dbo.game_schedule b
                    on a.game_id = b.game_id and a.team_id = b.away_id
                    left join lac_fullstack_dev.dbo.team c
                    on a.team_id = c.teamId
                    left join lac_fullstack_dev.dbo.team d
                    on b.away_id = d.teamId
                    left join lac_fullstack_dev.dbo.player e
                    on a.player_id = e.player_id
                    where b.game_date is not null
                    ),

-- -- i. Final table should be game date, team, opponent, player_name, period, stint_number, stint_start_time, stint_end_time
-- -- ii. Format the stint times in mm:ss so the start of the period is 12:00 and the end of the period is 00:00

-- select * from player_stints
-- where time_in = '12:00' and time_out = '00:00'

-- c. From you answer to 4.b, for each player, 
-- calculate the average number of stints a player has 

-- select b.game_date, b.team, a.player_name, a.avg_stints_per_game, b.avg_stint_length from (select avg(total_game_stints) as avg_stints_per_game, player_name from (select count(*) as total_game_stints , game_date, team, player_name  from player_stints
-- group by game_date, team, player_name) a
-- group by player_name) a
-- left join (select RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(datediff(minute,time_out,time_in)),0),108),5) as avg_stint_length, game_date, team, player_name  from player_stints
-- group by game_date, team, player_name) b
-- on a.player_name = b.player_name
-- where game_date = '2024-01-06 20:00:00' and team = 'Milwaukee Bucks'



-- and average stint length for a player for a given game.



-- d. Extend the query from 4.c to show columns for all games, in wins, in losses as well as a column that shows the difference in wins and losses
-- i.each set (all/wins/losses) should have # of games, average stint length, average number of stints


-- from sql query from #1
-- WITH 
game_stats as (select game_id, home_id, home_score, away_id, away_score,
                                case when home_score > away_score then 1
                                else 0
                                END 
                                as HOME_WIN,
                                case when home_score < away_score then 1
                                else 0
                                END 
                                as AWAY_WIN        


                                from lac_fullstack_dev.dbo.game_schedule a)




-- select b.game_id, b.team, c.teamId, a.player_name, a.avg_stints_per_game, b.avg_stint_length,
-- case when (c.teamid = d.home_id) and HOME_WIN = 1 then 1
-- when (c.teamid = d.away_id) and HOME_WIN = 1 then 1
-- else 0
-- end 
-- as win,
-- case when (c.teamid = d.home_id) and HOME_WIN = 0 then 1
-- when (c.teamid = d.away_id) and HOME_WIN = 0 then 1
-- else 0
-- end 
-- as lose
-- -- b.*,
-- -- d.*  
-- from (select avg(total_game_stints) as avg_stints_per_game, player_name 
--     from (select count(*) as total_game_stints , game_id, team, player_name  
--         from player_stints
--         group by game_id, team, player_name) a
--     group by player_name) a
-- left join (select RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(datediff(minute,time_out,time_in)),0),108),5) as avg_stint_length, game_id, team, player_name  from player_stints
-- group by game_id, team, player_name) b
-- on a.player_name = b.player_name
-- left join lac_fullstack_dev.dbo.team c
-- on b.team = c.teamName
-- left join (select * from game_stats) d
-- on b.game_id = d.game_id


select player_name, team, count(game_id) as total_games, avg(avg_stints_per_game) as avg_stints_per_game, RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(avg_stint_length),0),108),5)as avg_stint_length 
from (select b.game_id, b.team, c.teamId, a.player_name, a.avg_stints_per_game, b.avg_stint_length,
case when (c.teamid = d.home_id) and HOME_WIN = 1 then 1
when (c.teamid = d.away_id) and HOME_WIN = 1 then 1
else 0
end 
as win,
case when (c.teamid = d.home_id) and HOME_WIN = 0 then 1
when (c.teamid = d.away_id) and HOME_WIN = 0 then 1
else 0
end 
as lose
-- b.*,
-- d.*  
from (select avg(total_game_stints) as avg_stints_per_game, player_name 
    from (select count(*) as total_game_stints , game_id, team, player_name  
        from player_stints
        group by game_id, team, player_name) a
    group by player_name) a
-- left join (select RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(datediff(minute,time_out,time_in)),0),108),5) as avg_stint_length, game_id, team, player_name  from player_stints
left join (select avg(datediff(minute,time_out,time_in)) as avg_stint_length, game_id, team, player_name  from player_stints

group by game_id, team, player_name) b
on a.player_name = b.player_name
left join lac_fullstack_dev.dbo.team c
on b.team = c.teamName
left join (select * from game_stats) d
on b.game_id = d.game_id) a
where win = 1
group by player_name, team



"""
    try:
        connection.exceute_query(query_string_q4)
    except:
        error_value = str(sys.exc_info()[1])
        print(error_value)
