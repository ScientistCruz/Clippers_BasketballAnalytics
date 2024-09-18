-- Use lac_fullstack_dev
-- Go

-- CREATE VIEW question_04 AS

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
                    period, period as stint_number, RIGHT(CONVERT(CHAR(8),DATEADD(second,time_in,0),108),5) as stint_start_time, RIGHT(CONVERT(CHAR(8),DATEADD(second,time_out,0),108),5) as stint_end_time

                    from (select game_id, team_id, player_id, period,  max(time_in) as time_in, min(time_out) as time_out 
                        from (select a.* 
                            from (select team_id, game_id, player_id, [period], lineup_num, time_in, time_out
                        from lac_fullstack_dev.dbo.lineup) a
                            left join (select team_id, game_id, player_id, [period], lineup_num, time_in, time_out
                        from lac_fullstack_dev.dbo.lineup) b
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
                            from (select team_id, game_id, player_id, [period], lineup_num, time_in, time_out
                        from lac_fullstack_dev.dbo.lineup) a
                            left join (select team_id, game_id, player_id, [period], lineup_num, time_in, time_out
                        from lac_fullstack_dev.dbo.lineup) b
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
                    )
                    ,

-- -- i. Final table should be game date, team, opponent, player_name, period, stint_number, stint_start_time, stint_end_time
-- -- ii. Format the stint times in mm:ss so the start of the period is 12:00 and the end of the period is 00:00

-- select * from player_stints
-- where stint_start_time = '12:00' and stint_end_time = '00:00'

-- c. From you answer to 4.b, for each player, 
-- calculate the average number of stints a player has 

-- select b.game_id, b.team, a.player_name, a.avg_stints_per_game, b.avg_stint_length from (select avg(total_game_stints) as avg_stints_per_game, player_name from (select count(*) as total_game_stints , game_id, team, player_name  from player_stints
-- group by game_id, team, player_name) a
-- group by player_name) a
-- left join (select RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(datediff(minute,stint_end_time,stint_start_time)),0),108),5) as avg_stint_length, game_id, team, player_name  from player_stints
-- group by game_id, team, player_name) b
-- on a.player_name = b.player_name
-- -- where game_id = '102' and team = 'Milwaukee Bucks'



-- and average stint length for a player for a given game.



-- -- d. Extend the query from 4.c to show columns for all games, in wins, in losses as well as a column that shows the difference in wins and losses
-- -- i.each set (all/wins/losses) should have # of games, average stint length, average number of stints


-- -- from sql query from #1
-- -- WITH 
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
-- left join (select RIGHT(CONVERT(CHAR(8),DATEADD(second,avg(datediff(minute,stint_end_time,stint_start_time)),0),108),5) as avg_stint_length, game_id, team, player_name  from player_stints
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
left join (select avg(datediff(minute,stint_end_time,stint_start_time)) as avg_stint_length, game_id, team, player_name  from player_stints

group by game_id, team, player_name) b
on a.player_name = b.player_name
left join lac_fullstack_dev.dbo.team c
on b.team = c.teamName
left join (select * from game_stats) d
on b.game_id = d.game_id) a
-- where win = 1
group by player_name, team