-- Use lac_fullstack_dev
-- Go

-- CREATE VIEW question_03 AS

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


-- select * 
-- from(select b.teamName, a.* , 'home' as bb_type from home_BBs a
--     left join lac_fullstack_dev.dbo.team b
--     on a.team_id = b.teamId

--     union all

--     select b.teamName, a.*, 'away' as bb_type from away_BBs a
--     left join lac_fullstack_dev.dbo.team b
--     on a.team_id = b.teamId) a
-- order by game_date_g1_bb asc





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

-- with games_ro as (select *, ROW_NUMBER() OVER(PARTITION by team_id ORDER BY team_id, game_date) as rn from (select home_id as team_id, game_date from lac_fullstack_dev.dbo.game_schedule 
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
order by count(*) desc